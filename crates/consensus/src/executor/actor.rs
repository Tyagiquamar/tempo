//! Drives the actual execution forwarding blocks and setting forkchoice state.
//!
//! This agent ingests (monotonically) increasing finalized blocks from the
//! marshal actor and forwards them to the execution layer.
//!
//! In addition, the agent:
//!
//! 1. tracks the canonical (notarized) head of the simplex engine,
//! 2. drives the execution layer toward that notarized head,
//! 3. and validates and builds blocks.
//!
//! # Delivery and forkchoice are separate steps
//!
//! `newPayload` delivers a block body and never moves the head; notarized
//! blocks, finalized blocks, and validation probes are all deliveries.
//! `forkchoiceUpdated` moves the head and the finalized block, on a later
//! iteration, and only ever names blocks proven executed by a `VALID`
//! response for the block or its child. One update covers the completed deliveries.
//! Finalized blocks are acknowledged to the marshal actor once the update
//! finalizing them is accepted, and finality work is scheduled ahead of
//! notarized convergence.
//!
//! Verification probes the candidate first. SYNCING drives ancestor deliveries
//! backward until an engine answer or finality stops the walk, then the candidate
//! is re-probed for its verdict. Builds wait for their parent to become the head.
//!
//! # Notarized deliveries are retried, everything else is fatal
//!
//! A rejected notarized delivery is retried while the block stays above the
//! finalized tip. An `INVALID` finalized block is fatal, and so is any
//! forkchoice update not answered `VALID`: the executor's view of the
//! execution layer has diverged from it.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::B256;

use alloy_rpc_types_engine::{ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatusEnum};
use commonware_consensus::{
    CertifiableBlock as _, Heightable as _,
    marshal::Update,
    types::{Height, Round, View},
};
use commonware_cryptography::ed25519::PublicKey;
use commonware_runtime::{
    Clock, ContextCell, Handle, Metrics as RuntimeMetrics, Spawner, spawn_cell,
};
use commonware_utils::{Acknowledgement, acknowledgement::Exact};
use eyre::{OptionExt as _, Report, WrapErr as _, bail, ensure, eyre};
use futures::{
    FutureExt as _, StreamExt as _,
    channel::{
        mpsc::{self, UnboundedReceiver},
        oneshot,
    },
    future::BoxFuture,
    stream::FuturesUnordered,
};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use tempo_node::TempoExecutionData;
use tempo_payload_types::{TempoBuiltPayload, TempoPayloadAttributes};
use tokio::select;
use tracing::{
    Instrument as _, Level, Span, debug, error, error_span, info, info_span, instrument, warn,
};

use super::{
    Config, ExecutionLayer, Marshal,
    ingress::{Build, Command, Message, VerifyBlock},
};
use crate::{
    consensus::{Digest, block::Block},
    utils::OptionFuture,
};

#[cfg(test)]
mod tests;

/// How often to probe whether the execution layer is ready to process blocks.
const EXECUTION_LAYER_READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Back off when a complete ancestry walk still leaves the candidate SYNCING.
const VERIFICATION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Back off after a rejected build-parent delivery or transport failure.
const CONVERGENCE_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// How many new-payload requests the execution layer may have executed -
/// validations whatever their answer, `VALID` notarized and finalized
/// deliveries - are collected before a forkchoice update is forced. The
/// execution layer persists and prunes relative to its canonical head, so
/// blocks delivered ahead of the update that canonicalizes them stay in
/// memory. A queued consensus request runs ahead of the forced update, so
/// the bound is one more than this in practice.
const DELIVERIES_PER_FORKCHOICE_UPDATE: usize = 8;

pub(crate) struct Actor<TContext, TExecutionLayer, TMarshal> {
    context: ContextCell<TContext>,

    /// A handle to the execution node layer. Used to forward finalized blocks
    /// and to update the canonical chain by sending forkchoice updates.
    execution_node: TExecutionLayer,

    /// Highest finalized height the executor should backfill to on startup so
    /// that CL and EL have a consistent view.
    finalized_floor: Height,

    /// The channel over which the agent will receive new commands from the
    /// application actor.
    mailbox: mpsc::UnboundedReceiver<Message>,

    /// The mailbox of the marshal actor. Used to backfill finalized blocks
    /// on startup and to fetch missing notarized block bodies.
    marshal: TMarshal,

    /// The interval at which to send a forkchoice update heartbeat to the
    /// execution layer.
    fcu_heartbeat_interval: Duration,

    /// The timer for the next FCU heartbeat.
    ///
    /// Armed when no execution-layer work can start, including while a build
    /// waits for its parent. This also wakes convergence retries after rejection.
    fcu_heartbeat_timer: OptionFuture<BoxFuture<'static, ()>>,

    /// Finalized blocks waiting to be delivered to the execution layer.
    pending_finalizations: VecDeque<FinalizedBlockRequest>,

    /// Finalized blocks the execution layer has accepted, waiting for the
    /// forkchoice update that finalizes them before they are acknowledged
    /// to the marshal actor. In height order.
    pending_acknowledgements: VecDeque<FinalizedBlockRequest>,

    /// New-payload requests the execution layer may have executed since the
    /// last forkchoice update, see [`DELIVERIES_PER_FORKCHOICE_UPDATE`].
    deliveries_since_forkchoice: usize,

    /// The newest round observed through build and verify contexts or
    /// finalized-tip reports. Requests can arrive out of order; an older
    /// round's parent must not supersede a newer one's. Retained independently
    /// of request cancellation and completion.
    latest_consensus_round: Round,

    /// The latest not-yet-started consensus request - validating a proposed
    /// block or building one - keyed by its round. The two kinds share one
    /// slot because a node either verifies or proposes in a round, never
    /// both. A request from a newer round supersedes a queued older one;
    /// requests at or below the queued round are dropped on arrival. Either
    /// way, dropping a request's response channel signals the failure to its
    /// subscriber.
    pending_consensus_request: Option<(Round, ConsensusRequest)>,

    /// The started verification between engine calls. It retains the candidate
    /// while ancestors are fetched and delivered. During an engine call the
    /// execution task owns it instead; newer requests still share one queued slot.
    verification: Option<PayloadWalk>,
    verification_retry: OptionFuture<BoxFuture<'static, ()>>,
    pending_verification_block: OptionFuture<PendingNotarizedBlock>,

    /// Uses the same walk to execute a build's parent before requesting a payload.
    convergence: Option<PayloadWalk>,
    convergence_retry: OptionFuture<BoxFuture<'static, ()>>,

    /// The single execution-layer request currently being driven in the background.
    execution_task: OptionFuture<ExecutionTask>,

    /// The build parent's body, or an ancestor requested by its delivery walk.
    /// Verification has its own fetch so either walk can wait independently.
    pending_convergence_block: OptionFuture<PendingNotarizedBlock>,

    /// Payload build jobs currently being driven to completion.
    ///
    /// Each job resolves a payload from the execution layer's payload builder
    /// and delivers it to the subscriber that requested the build. If the
    /// subscriber dropped its receiver in the meantime, the built payload is
    /// discarded. A delivered block is handed back as the job's output so
    /// that its body can be retained for a later build: the proposer is never
    /// asked to verify its own proposal, so no validation request delivers it.
    payload_jobs: FuturesUnordered<BoxFuture<'static, Option<Arc<Block>>>>,

    /// The last accepted forkchoice and the two distinct finality watermarks.
    local_state: LocalState,
    delivered_finalized: (Height, Digest),
    network_finalized_tip: (Round, Height, Digest),

    /// The parent selected by the latest consensus context. Verification proves
    /// its readiness; builds may also need to fetch and deliver it themselves.
    pending_head: PendingHead,

    /// Heights proven executed by VALID responses. No bodies or ancestry links
    /// are retained, and this cache never decides which ancestor to deliver.
    known_blocks: HashMap<Digest, Height>,

    /// Own proposals have not been delivered through verification. Retain their
    /// bodies until finality so a later build can deliver its selected parent.
    built_blocks: HashMap<Digest, Arc<Block>>,

    /// The node's ed25519 public key if the node is participating in
    /// consensus. Not set if not, for example for followers.
    public_key: Option<PublicKey>,

    metrics: Metrics,
}

#[derive(Clone)]
struct Metrics {
    /// Number of finalized blocks whose proposer matches this node's public key.
    finalized_blocks_proposed_by_self: commonware_runtime::telemetry::metrics::Registered<Counter>,
    /// Height distance from the locally canonicalized finalized tip up to
    /// the network's finalized tip: the undelivered finalized backlog.
    finalization_lag: commonware_runtime::telemetry::metrics::Registered<Gauge>,
    /// Height distance from the execution layer's head to the pending head:
    /// the convergence backlog. Negative when consensus re-anchored below
    /// the head; holds its last value while the pending head's height is
    /// unknown (its body has not arrived yet).
    convergence_depth: commonware_runtime::telemetry::metrics::Registered<Gauge>,
}

impl Metrics {
    fn init<TContext>(context: &TContext) -> Self
    where
        TContext: RuntimeMetrics,
    {
        let finalized_blocks_proposed_by_self = context.register(
            "finalized_blocks_proposed_by_self",
            "number of finalized blocks whose proposer matches this node's public key",
            Counter::default(),
        );
        let finalization_lag = context.register(
            "finalization_lag",
            "height distance from the locally canonicalized finalized tip up to the \
            network's finalized tip",
            Gauge::default(),
        );
        let convergence_depth = context.register(
            "convergence_depth",
            "height distance from the execution layer's head to the pending head \
            (negative after a re-anchor below the head)",
            Gauge::default(),
        );
        Self {
            finalized_blocks_proposed_by_self,
            finalization_lag,
            convergence_depth,
        }
    }

    fn observe(&self, local: LocalState, finalized: Height, pending_height: Option<Height>) {
        self.finalization_lag
            .set(finalized.get().saturating_sub(local.finalized.0.get()) as i64);
        if let Some(height) = pending_height {
            self.convergence_depth
                .set(height.get() as i64 - local.head.0.get() as i64);
        }
    }
}

impl<TContext, TExecutionLayer, TMarshal> Actor<TContext, TExecutionLayer, TMarshal>
where
    TContext: Clock + RuntimeMetrics + Spawner,
    TExecutionLayer: ExecutionLayer,
    TMarshal: Marshal,
{
    pub(super) fn init(
        context: TContext,
        config: super::Config<TExecutionLayer, TMarshal>,
        mailbox: UnboundedReceiver<super::ingress::Message>,
    ) -> eyre::Result<Self> {
        let Config {
            execution_node,
            finalized_floor,
            finalized_tip,
            marshal,
            fcu_heartbeat_interval,
            public_key,
        } = config;
        ensure!(
            finalized_tip.1 >= finalized_floor,
            "finalized tip height `{}` is below the finalized floor `{finalized_floor}`",
            finalized_tip.1,
        );
        let metrics = Metrics::init(&context);

        let execution_finalized_num_hash = execution_node.finalized_num_hash();

        // The finalized point the executor starts from. Normally this is the
        // execution layer's own finalized tip, from which the startup
        // backfill climbs to the finalized floor. The floor can also sit
        // *below* the execution layer's finality: a restored consensus
        // snapshot may anchor below the finality of the execution database
        // it is restored next to. The marshal then re-delivers finalized
        // blocks from the floor, so the tracked state must start there for
        // the re-delivery to line up; the already-finalized blocks are
        // acknowledged without involving the execution layer (see
        // [`Self::handle_finalized_delivered`]).
        let finalized = if finalized_floor.get() < execution_finalized_num_hash.number {
            let digest = execution_node
                .canonical_block_hash(finalized_floor.get())
                .wrap_err_with(|| {
                    format!(
                        "failed reading canonical execution block hash at the \
                        finalized floor height `{finalized_floor}`"
                    )
                })?
                .ok_or_eyre(format!(
                    "no canonical execution block hash at the finalized floor \
                    height `{finalized_floor}`, even though the floor is below \
                    the execution layer's finalized height `{}`",
                    execution_finalized_num_hash.number,
                ))?;
            (finalized_floor, Digest(digest))
        } else {
            (
                Height::new(execution_finalized_num_hash.number),
                Digest(execution_finalized_num_hash.hash),
            )
        };

        // The forkchoice state the executor starts from: the startup
        // finalized point for both head and finalized - the two are not
        // differentiated at startup. The head converges onto the notarized
        // tip through normal operation.
        let local_state = LocalState {
            head: finalized,
            finalized,
        };

        Ok(Self {
            context: ContextCell::new(context),
            execution_node,
            finalized_floor,
            mailbox,
            marshal,
            fcu_heartbeat_interval,
            fcu_heartbeat_timer: OptionFuture::none(),

            pending_finalizations: VecDeque::new(),
            pending_acknowledgements: VecDeque::new(),
            deliveries_since_forkchoice: 0,
            latest_consensus_round: finalized_tip.0,
            pending_consensus_request: None,
            verification: None,
            convergence: None,
            convergence_retry: OptionFuture::none(),
            pending_verification_block: OptionFuture::none(),
            verification_retry: OptionFuture::none(),

            execution_task: OptionFuture::none(),
            pending_convergence_block: OptionFuture::none(),
            payload_jobs: FuturesUnordered::new(),

            local_state,
            delivered_finalized: local_state.finalized,
            network_finalized_tip: finalized_tip,
            pending_head: PendingHead::finalized(finalized_tip),
            known_blocks: HashMap::new(),
            built_blocks: HashMap::new(),

            public_key,
            metrics,
        })
    }

    pub(crate) fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        if let Err(error) = self.wait_for_execution_layer().await {
            error_span!("shutdown").in_scope(|| {
                error!(
                    %error,
                    "failed waiting for execution layer readiness",
                )
            });
            return;
        }

        if let Err(error) = self.backfill_to_finalized_floor().await {
            error_span!("shutdown").in_scope(|| {
                error!(
                    %error,
                    "executor failed startup backfill",
                )
            });
            return;
        }

        info_span!("start").in_scope(|| {
            let canonicalized = self.local_state;
            info!(
                finalized_height = %canonicalized.finalized.0,
                finalized_digest = %canonicalized.finalized.1,
                head_height = %canonicalized.head.0,
                head_digest = %canonicalized.head.1,
                "entering executor loop",
            );
        });

        loop {
            self.prune_finalized();
            self.metrics.observe(
                self.local_state,
                self.network_finalized_tip.1,
                self.pending_head.height,
            );
            self.prepare_walks();
            self.update_block_fetches();

            if let Err(error) = self.start_next_execution_task() {
                error_span!("shutdown").in_scope(|| {
                    error!(
                        %error,
                        "executor failed scheduling execution-layer work; \
                        shutting down to prevent consensus-execution divergence"
                    )
                });
                break;
            }
            self.update_fcu_heartbeat_timer();

            select! {
                biased;

                finished = &mut self.execution_task => {
                    if let Err(error) = self.handle_execution_task_finished(finished) {
                        error_span!("shutdown").in_scope(|| error!(
                            %error,
                            "executor encountered fatal execution-layer update error; \
                            shutting down to prevent consensus-execution divergence"
                        ));
                        break;
                    }
                }

                () = async {
                    match self.verification.as_mut() {
                        Some(verification) => verification.request.cancellation().await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.verification = None;
                    self.verification_retry = OptionFuture::none();
                }

                () = &mut self.verification_retry => {
                    if let Some(walk) = self.verification.as_mut() {
                        walk.retry();
                    }
                }
                () = &mut self.convergence_retry => {
                    if let Some(walk) = self.convergence.as_mut() {
                        walk.retry();
                    }
                }
                (digest, round, block) = &mut self.pending_verification_block => {
                    Self::handle_fetched_parent(&mut self.verification, digest, round, block);
                }

                Some(delivered) = self.payload_jobs.next() => {
                    if let Some(block) = delivered {
                        // The application received the built block and may
                        // propose it; keep the body so the block can be
                        // forwarded to the execution layer once a later
                        // context proves it notarized.
                        self.built_blocks.insert(block.digest(), block);
                    }
                }

                (digest, round, block) = &mut self.pending_convergence_block => {
                    self.handle_fetched_convergence_block(digest, round, block);
                }

                msg = self.mailbox.next() => {
                    let Some(msg) = msg else { break; };
                    if let Err(error) = self.handle_message(msg) {
                        error_span!("shutdown").in_scope(|| error!(
                            %error,
                            "executor failed handling message; \
                            shutting down to prevent consensus-execution divergence"
                        ));
                        break;
                    }
                },

                _ = (&mut self.fcu_heartbeat_timer).fuse() => {
                    if let Err(error) = self.send_forkchoice_update_heartbeat() {
                        error_span!("shutdown").in_scope(|| error!(
                            %error,
                            "executor failed scheduling forkchoice update heartbeat; \
                            shutting down to prevent consensus-execution divergence"
                        ));
                        break;
                    }
                },
            }
        }
    }

    #[instrument(
        skip_all,
        fields(
            task_type = task.task_type.name(),
        ),
    )]
    fn set_execution_task(&mut self, mut task: ExecutionTask) {
        task.span = Span::current();
        assert!(
            self.execution_task.replace(task).is_none(),
            "invariant violation: must not replace an in-flight execution task"
        );
        info!("execution task scheduled");
    }

    /// Interprets the answer of a finished execution task: this is the one
    /// place where answers are turned into decisions - what to mark
    /// delivered, what to withhold, what to acknowledge, and what is fatal.
    /// There is only one task at a time, so the tracked state at completion
    /// is the state the task ran on top of.
    #[instrument(
        parent = &finished.span,
        skip_all,
        fields(
            task_type = finished.task_type.name(),
            target = ?finished.target(),
            outcome = finished.outcome.name(),
        ),
        err,
    )]
    fn handle_execution_task_finished(
        &mut self,
        finished: ExecutionTaskFinished,
    ) -> eyre::Result<()> {
        info!(
            elapsed = %tempo_telemetry_util::display_duration(finished.started_at.elapsed()),
            "execution task finished"
        );
        let ExecutionTaskFinished { outcome, .. } = finished;
        match outcome {
            ExecutionTaskOutcome::Validated { request, status } => {
                // A failed validation is logged by the handler; it is not
                // fatal, consensus treats it as a rejected proposal.
                let _logged = self.handle_validated(request, status);
            }
            ExecutionTaskOutcome::FinalizedDelivered { request, status } => {
                self.handle_finalized_delivered(request, status)?
            }
            ExecutionTaskOutcome::Forkchoice {
                target,
                build,
                response,
            } => self.handle_forkchoice_response(target, build, response)?,
        }
        Ok(())
    }

    /// Only the candidate's VALID/INVALID answer resolves verification. SYNCING
    /// walks one ancestor backward; an ancestor's terminal answer re-probes the
    /// original candidate. No validation outcome changes the pending head.
    #[instrument(skip_all, err(level = Level::WARN))]
    fn handle_validated(
        &mut self,
        request: Option<PayloadWalk>,
        status: eyre::Result<(PayloadStatusEnum, Duration)>,
    ) -> eyre::Result<()> {
        // Every probe counts towards the forced forkchoice update, whatever
        // it answered: the execution layer may execute an abandoned probe
        // after the subscriber left, and buffers a `SYNCING` one to execute
        // it once the parent arrives, without a `VALID` answer for either.
        self.deliveries_since_forkchoice += 1;
        let Some(mut verification) = request else {
            return Ok(());
        };
        let (status, duration) = match status {
            Ok(result) => result,
            Err(error) => {
                if verification.request.response.is_none() {
                    self.pause_convergence(verification);
                }
                return Err(error.wrap_err("failed delivering block"));
            }
        };
        verification.duration += duration;
        let digest = verification.cursor.digest();
        let verdict = match status {
            PayloadStatusEnum::Valid => {
                self.record_valid_block(&verification.cursor);
                if verification.is_candidate() {
                    Some(verification.duration)
                } else {
                    verification.reprobe();
                    self.retain_walk(verification);
                    return Ok(());
                }
            }
            PayloadStatusEnum::Invalid { validation_error } => {
                info!(%digest, validation_error, "execution layer rejected the block");
                if !verification.is_candidate() {
                    // Learn the candidate's own verdict, including invalidity
                    // discovered while the engine connected buffered descendants.
                    verification.reprobe();
                    self.retain_walk(verification);
                    return Ok(());
                }
                None
            }
            PayloadStatusEnum::Syncing => {
                self.known_blocks.remove(&digest);
                self.known_blocks
                    .remove(&verification.cursor.parent_digest());
                verification.step = if verification.reprobed {
                    WalkStep::Retry
                } else {
                    WalkStep::Parent
                };
                self.retain_walk(verification);
                return Ok(());
            }
            PayloadStatusEnum::Accepted => {
                if verification.request.response.is_none() {
                    self.pause_convergence(verification);
                }
                bail!("payload was accepted without execution while delivering block");
            }
        };
        if let Some(response) = verification.request.response.take() {
            if response.send(verdict).is_err() {
                info!("verification subscriber went away before the verdict was delivered");
            }
        } else if verdict.is_none() {
            self.pause_convergence(verification);
        }
        Ok(())
    }

    fn retain_walk(&mut self, walk: PayloadWalk) {
        if walk.request.response.is_some() {
            self.verification = Some(walk);
        } else if walk.request.block.digest() == self.pending_head.digest {
            self.convergence = Some(walk);
        }
    }

    fn pause_convergence(&mut self, mut walk: PayloadWalk) {
        walk.step = WalkStep::Retry;
        if walk.request.block.digest() == self.pending_head.digest {
            self.convergence = Some(walk);
            self.convergence_retry
                .replace(self.context.sleep(CONVERGENCE_RETRY_INTERVAL).boxed());
        }
    }

    /// Only SYNCING asks for an ancestor. Finalized history is delivered by the
    /// finalization pipeline, even when its boundary advances during a fetch.
    fn prepare_walks(&mut self) {
        if self
            .verification
            .as_ref()
            .is_some_and(|walk| walk.request.is_canceled())
        {
            self.verification = None;
            self.verification_retry = OptionFuture::none();
        }
        for (walk, retry) in [
            (&mut self.verification, &mut self.verification_retry),
            (&mut self.convergence, &mut self.convergence_retry),
        ] {
            if let Some(walk) = walk {
                if walk.step == WalkStep::Parent
                    && (walk.parent().1 == self.network_finalized_tip.2
                        || walk.cursor.height().get().saturating_sub(1)
                            <= self.network_finalized_tip.1.get())
                {
                    walk.step = WalkStep::Retry;
                }
                if walk.step == WalkStep::Retry && retry.is_none() {
                    retry.replace(self.context.sleep(VERIFICATION_RETRY_INTERVAL).boxed());
                }
            }
        }
    }

    fn retry_walks(&mut self) {
        for (walk, retry) in [
            (&mut self.verification, &mut self.verification_retry),
            (&mut self.convergence, &mut self.convergence_retry),
        ] {
            if let Some(walk) = walk
                && walk.step == WalkStep::Retry
            {
                walk.retry();
                *retry = OptionFuture::none();
            }
        }
    }

    fn record_valid_block(&mut self, block: &Block) {
        // VALID proves both the submitted block and its parent executed. Only
        // the consensus-selected parent may become the convergence target.
        for (digest, height) in [
            (block.digest(), block.height()),
            (
                block.parent_digest(),
                Height::new(block.height().get().saturating_sub(1)),
            ),
        ] {
            if height > self.network_finalized_tip.1 {
                self.known_blocks.insert(digest, height);
            }
            if digest == self.pending_head.digest {
                self.pending_head.height = Some(height);
            }
        }
    }

    fn known_height(&self, digest: Digest) -> Option<Height> {
        [
            self.local_state.head,
            self.local_state.finalized,
            self.delivered_finalized,
        ]
        .into_iter()
        .find_map(|(height, known)| (known == digest).then_some(height))
        .or_else(|| self.known_blocks.get(&digest).copied())
    }

    fn prune_finalized(&mut self) {
        let (round, height, digest) = self.network_finalized_tip;
        debug_assert!(self.local_state.finalized.0 <= height);
        self.known_blocks.retain(|_, known| *known > height);
        self.built_blocks.retain(|_, block| block.height() > height);
        if self.pending_head.round <= round && self.pending_head.digest != digest {
            self.pending_head = PendingHead::finalized(self.network_finalized_tip);
        }
        if self.pending_head.digest == digest {
            self.pending_head.height = Some(height);
        }
        if !self.pending_head.requires_delivery
            || self
                .convergence
                .as_ref()
                .is_some_and(|walk| walk.request.block.digest() != self.pending_head.digest)
            || self.known_height(self.pending_head.digest).is_some()
        {
            self.convergence = None;
            self.convergence_retry = OptionFuture::none();
        }
    }

    /// An accepted forkchoice update becomes the tracked state (mutated only
    /// here), and so does a stale one that was not submitted (`None`). A
    /// rejected one is fatal: every target named is a block the execution
    /// layer accepted, so the executor's view of the execution layer has
    /// diverged from it. Finalized blocks the update covers are
    /// acknowledged; a build it carried is driven to completion.
    fn handle_forkchoice_response(
        &mut self,
        target: LocalState,
        build: Option<(Span, oneshot::Sender<TempoBuiltPayload>)>,
        response: Option<eyre::Result<ForkchoiceUpdated>>,
    ) -> eyre::Result<()> {
        let Some(response) = response else {
            // Nothing was submitted: the execution layer is past this
            // finality already, and it never moves finality backwards. The
            // tracked state catches up to the target anyway.
            //
            // NOTE: this records a head the execution layer was never told
            // about. It is sound because the head lies on the pending head's
            // ancestry, and consensus only reports pending heads above the
            // execution layer's finality, so that ancestry runs through the
            // finalized chain: the first non-stale update names a head that
            // descends from its finalized block. The tracked head starts at
            // the finalized floor rather than at the execution layer's own
            // head because that head may sit on a branch nullified while the
            // node was down.
            if build.is_some() {
                // Dropping the build's response channel signals the failure
                // to the subscriber.
                info!("tracked finality is below the execution layer's; dropping the build");
            }
            self.local_state = target;
            self.acknowledge_finalized();
            return Ok(());
        };
        let diverged = || {
            format!(
                "forkchoice update onto head `{}` at height `{}` and finalized block `{}` at \
                height `{}` failed; the executor's view of the execution layer has diverged \
                from the execution layer",
                target.head.1, target.head.0, target.finalized.1, target.finalized.0,
            )
        };
        let response = response.wrap_err_with(diverged)?;
        if !response.is_valid() {
            return Err(Report::msg(response.payload_status)).wrap_err_with(diverged);
        }

        self.local_state = target;
        self.acknowledge_finalized();

        // Dropping the build's response channel signals the failure to the
        // subscriber.
        match (build, response.payload_id) {
            (Some((cause, response)), Some(payload_id)) => {
                let job = StartPayloadJob {
                    cause,
                    payload_id,
                    response,
                };
                self.payload_jobs
                    .push(run_payload_job(self.execution_node.clone(), job).boxed());
            }
            (Some(_dropped_to_signal_failure), None) => {
                warn!("execution layer did not return a payload id for the build request");
            }
            (None, _) => {}
        }
        Ok(())
    }

    /// A non-`VALID` answer is fatal. Otherwise the block becomes the next
    /// finalized target and is acknowledged once the forkchoice update
    /// finalizing it lands - or right away if the execution layer already
    /// finalized it (a re-delivery). The marshal actor delivers the
    /// finalized chain in order; that order is trusted, not checked.
    #[instrument(
        skip_all,
        fields(
            block.digest = %request.block.digest(),
            block.height = %request.block.height(),
        ),
        err,
    )]
    fn handle_finalized_delivered(
        &mut self,
        request: FinalizedBlockRequest,
        status: eyre::Result<PayloadStatusEnum>,
    ) -> eyre::Result<()> {
        let block = request.block.as_ref();
        debug_assert!(
            block.height() >= self.delivered_finalized.0,
            "finalized blocks are delivered in height order",
        );
        match status {
            Ok(PayloadStatusEnum::Valid) => {}
            Ok(status) => {
                bail!(
                    "payload status of finalized block `{}` at height `{}` was \
                    not valid: {status}",
                    block.digest(),
                    block.height(),
                );
            }
            Err(error) => {
                return Err(error.wrap_err(format!(
                    "failed delivering finalized block `{}` at height `{}`",
                    block.digest(),
                    block.height(),
                )));
            }
        }

        if block.height() > self.delivered_finalized.0 {
            self.delivered_finalized = (block.height(), block.digest());
        }
        self.deliveries_since_forkchoice += 1;
        self.retry_walks();
        if block.height() <= self.local_state.finalized.0 {
            // NOTE: this block is already final on the execution layer. This
            // can happen if marshal is anchored below the EL and delivers
            // finalized blocks the EL already knows about. In this case, it
            // makes sense to ACK immediately rather than wait for an FCU
            // sweep.
            // The execution layer confirms it is the block it finalized at
            // this height before it is acknowledged.
            let canonical = self
                .execution_node
                .canonical_block_hash(block.height().get())
                .wrap_err_with(|| {
                    format!(
                        "failed reading canonical execution block hash at finalized block \
                        height `{}`",
                        block.height(),
                    )
                })?;
            ensure!(
                canonical == Some(block.digest().0),
                "re-delivered finalized block `{}` at height `{}` conflicts with the \
                execution layer's canonical block `{canonical:?}` at the same height, which \
                the execution layer already considers final",
                block.digest(),
                block.height(),
            );
            self.acknowledge(request);
        } else {
            self.pending_acknowledgements.push_back(request);
        }
        Ok(())
    }

    /// Acknowledges the queued blocks the last accepted forkchoice update
    /// finalized. Deliveries are in chain order, so height identifies them.
    ///
    /// NOTE: the tracked state is the reference, not the execution layer's
    /// own finalized marker. An update whose head is a canonical ancestor of
    /// the execution layer's head is answered `VALID` without the marker
    /// moving, and after a restart every update is of that kind until the
    /// head catches up: waiting for the marker would never acknowledge and
    /// stall the marshal actor. The blocks are canonical and held by the
    /// execution layer either way; the marker follows with the first update
    /// that moves the head onto a new block.
    fn acknowledge_finalized(&mut self) {
        let finalized = self.local_state.finalized.0;
        while let Some(request) = self.pending_acknowledgements.front() {
            if request.block.height() > finalized {
                break;
            }
            let Some(request) = self.pending_acknowledgements.pop_front() else {
                break;
            };
            self.acknowledge(request);
        }
    }

    /// Acknowledges a block the execution layer finalized to the marshal actor.
    fn acknowledge(&self, request: FinalizedBlockRequest) {
        let FinalizedBlockRequest {
            cause,
            block,
            acknowledgment,
        } = request;
        let _entered = cause.enter();
        if let Some(public_key) = self.public_key.as_ref()
            && block
                .header()
                .consensus_context
                .is_some_and(|context| context.proposer.to_inner() == *public_key)
        {
            self.metrics.finalized_blocks_proposed_by_self.inc();
        }
        info!(
            block.digest = %block.digest(),
            block.height = %block.height(),
            "finalized block is final on the execution layer; acknowledging it",
        );
        acknowledgment.acknowledge();
    }

    /// Waits until reth is ready to process blocks by repeatedly reaffirming the execution layer's
    /// own forkchoice state.
    ///
    /// Reth returns `SYNCING` for every forkchoice update while its backfill pipeline is active.
    /// Because the probe uses the execution layer's current head, safe block, and finalized block,
    /// it is non-destructive and cannot report `SYNCING` due to a detached CL-provided head. Once
    /// the probe returns `VALID`, later `SYNCING` responses while forwarding finalized blocks can
    /// be treated as invalid state.
    async fn wait_for_execution_layer(&mut self) -> eyre::Result<()> {
        for attempts in 1_u64.. {
            if self
                .execution_node
                .is_ready()
                .instrument(info_span!("check_execution_layer_readiness", attempts))
                .await?
            {
                break;
            }
            self.context
                .sleep(EXECUTION_LAYER_READY_POLL_INTERVAL)
                .await;
        }

        Ok(())
    }

    /// Climbs from the tracked finalized state to the finalized floor
    /// before entering the loop, through the regular finalization tasks
    /// and their outcome handling, awaited in place. Every
    /// [`DELIVERIES_PER_FORKCHOICE_UPDATE`] delivered blocks, and at the
    /// floor, a forkchoice update finalizes them.
    #[instrument(skip_all, err)]
    async fn backfill_to_finalized_floor(&mut self) -> eyre::Result<()> {
        let start = self.local_state.finalized.0.get() + 1;
        let end = self.finalized_floor.get();
        let heights = start..=end;
        if !heights.is_empty() {
            info!(
                start = *heights.start(),
                end = *heights.end(),
                "backfilling finalized blocks before entering executor loop"
            );
        }
        for height in heights {
            let span = info_span!("backfill_on_start", %height);
            let block = get_block(
                self.marshal.clone(),
                self.execution_node.clone(),
                Height::new(height),
            )
            .await
            .wrap_err_with(|| format!("failed backfilling block for height `{height}`"))?;

            let (ack, _wait) = Exact::handle();
            let request = FinalizedBlockRequest {
                cause: span,
                block: Arc::new(block),
                acknowledgment: ack,
            };
            let fut = execute_finalization(self.execution_node.clone(), request);
            self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Finalize, fut));
            let finished = (&mut self.execution_task).await;
            self.handle_execution_task_finished(finished)
                .wrap_err_with(|| {
                    format!(
                        "failed forwarding backfilled finalized block at height `{height}` \
                        to execution layer"
                    )
                })?;

            if (self.deliveries_since_forkchoice >= DELIVERIES_PER_FORKCHOICE_UPDATE
                || height == end)
                && self.start_forkchoice_update()?
            {
                let finished = (&mut self.execution_task).await;
                self.handle_execution_task_finished(finished)
                    .wrap_err_with(|| {
                        format!(
                            "failed finalizing backfilled finalized block at height `{height}` \
                            on the execution layer"
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn arm_fcu_heartbeat_timer(&mut self) {
        if !self.fcu_heartbeat_timer.is_none() {
            return;
        }
        self.fcu_heartbeat_timer
            .replace(self.context.sleep(self.fcu_heartbeat_interval).boxed());
    }

    fn disarm_fcu_heartbeat_timer(&mut self) {
        self.fcu_heartbeat_timer = OptionFuture::none();
    }

    fn update_fcu_heartbeat_timer(&mut self) {
        if self.execution_task.is_none() && self.pending_finalizations.is_empty() {
            self.arm_fcu_heartbeat_timer();
        } else {
            self.disarm_fcu_heartbeat_timer();
        }
    }

    /// Re-affirms the tracked forkchoice state, unless the scheduler finds
    /// real work to do first.
    #[instrument(skip_all)]
    fn send_forkchoice_update_heartbeat(&mut self) -> eyre::Result<()> {
        // A waiting build must not suppress retries of its parent's delivery.
        // Give convergence priority over re-affirming the tracked state.
        if !self.execution_task.is_none() {
            return Ok(());
        }

        self.start_next_execution_task()?;
        if !self.execution_task.is_none() {
            return Ok(());
        }

        let target = self.local_state;
        let fut = execute_forkchoice(self.execution_node.clone(), Span::current(), target, None);
        self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Heartbeat, fut));
        Ok(())
    }

    fn handle_message(&mut self, message: Message) -> eyre::Result<()> {
        let cause = message.cause;
        match message.command {
            Command::Build(build) => {
                self.record_convergence_target(build.context.round, build.context.parent, true);
                // Cancellation discards the build work, not its parent target.
                if build.response.is_canceled() {
                    return Ok(());
                }
                queue_consensus_request(
                    &mut self.pending_consensus_request,
                    build.context.round,
                    ConsensusRequest::Build { cause, build },
                );
            }
            Command::Finalize(finalized) => match *finalized {
                Update::Tip(round, height, digest) => {
                    self.record_convergence_target(round, (round.view(), digest), false);
                    // A now-stale in-flight body fetch is dropped by
                    // `update_block_fetches` on the next loop
                    // iteration.
                    if round > self.network_finalized_tip.0 {
                        self.network_finalized_tip = (round, height, digest);
                    }
                }
                Update::Block(block, acknowledgement) => {
                    self.pending_finalizations.push_back(FinalizedBlockRequest {
                        cause,
                        block,
                        acknowledgment: acknowledgement,
                    });
                }
            },
            Command::VerifyBlock(request) => {
                let VerifyBlock {
                    context,
                    block,
                    validator_set,
                    response,
                } = *request;
                self.record_convergence_target(context.round, context.parent, false);
                queue_consensus_request(
                    &mut self.pending_consensus_request,
                    context.round,
                    ConsensusRequest::Verify(PayloadRequest {
                        parent_round: Round::new(context.round.epoch(), context.parent.0),
                        cause,
                        block,
                        validator_set,
                        response: Some(response),
                    }),
                );
            }
        }
        Ok(())
    }

    /// Records the newest observed consensus round and its convergence target.
    /// Build and verify requests select their parent; finalized-tip reports
    /// select the finalized block itself. A later round can select an older
    /// parent after nullifications, so the observed round orders targets.
    ///
    /// NOTE: the first proposed block of an epoch will always have a round
    /// `round = (<epoch>, <view>) = (<epoch>, 0)`. This is not a real round
    /// and hinges on the assumption that in order to verify or propose blocks
    /// for `<epoch>`, the node must have finalized the boundary block of
    /// `<epoch>`, which is exactly that parent block. In fact, a node will not
    /// start a simplex engine for `<epoch>` if it does not have this block.
    #[instrument(
        skip_all,
        fields(
            %round,
            latest_consensus_round = %self.latest_consensus_round,
            target.view = %target.0,
            target.digest = %target.1,
            requires_delivery,
        ),
    )]
    fn record_convergence_target(
        &mut self,
        round: Round,
        target: (View, Digest),
        requires_delivery: bool,
    ) {
        if round >= self.latest_consensus_round {
            info!("updating convergence target");
            self.latest_consensus_round = round;
            let height = self.known_height(target.1).or_else(|| {
                (self.pending_head.digest == target.1)
                    .then_some(self.pending_head.height)
                    .flatten()
            });
            self.pending_head = PendingHead {
                round: Round::new(round.epoch(), target.0),
                digest: target.1,
                height,
                requires_delivery,
            };
        }
    }

    /// Verification and build-parent walks own independent fetches. No ancestry
    /// is prefetched: the next parent is requested only after SYNCING.
    fn update_block_fetches(&mut self) {
        let next = self
            .verification
            .as_ref()
            .filter(|walk| walk.step == WalkStep::Parent)
            .map(PayloadWalk::parent);
        update_block_fetch(&self.marshal, &mut self.pending_verification_block, next);

        let delivering = self
            .execution_task
            .as_ref()
            .is_some_and(|task| matches!(task.task_type, ExecutionTaskType::Deliver));
        let needs_parent = !delivering
            && self.pending_head.requires_delivery
            && self.known_height(self.pending_head.digest).is_none()
            && self.pending_head.digest != self.network_finalized_tip.2;
        if needs_parent
            && self.convergence.is_none()
            && let Some(block) = self.built_blocks.get(&self.pending_head.digest)
        {
            self.pending_head.height = Some(block.height());
            self.convergence = Some(PayloadWalk::converge(block.clone()));
        }
        let next = if needs_parent {
            match &self.convergence {
                Some(walk) if walk.step == WalkStep::Parent => Some(walk.parent()),
                Some(_) => None,
                None => Some((self.pending_head.round, self.pending_head.digest)),
            }
        } else {
            None
        };
        update_block_fetch(&self.marshal, &mut self.pending_convergence_block, next);
    }

    #[instrument(skip_all, fields(%digest, %round))]
    fn handle_fetched_parent(
        walk: &mut Option<PayloadWalk>,
        digest: Digest,
        round: Round,
        block: Option<Arc<Block>>,
    ) {
        if let Some(active) = walk {
            match block {
                Some(block)
                    if block.digest() == digest
                        && block.height().get().checked_add(1)
                            == Some(active.cursor.height().get()) =>
                {
                    active.cursor = block;
                    active.step = WalkStep::Probe;
                }
                Some(_) => {
                    warn!("fetched ancestor does not match the requested parent");
                    *walk = None;
                }
                None => warn!("marshal dropped the ancestor subscription; retrying the fetch"),
            }
        }
    }

    #[instrument(skip_all, fields(%digest, %round))]
    fn handle_fetched_convergence_block(
        &mut self,
        digest: Digest,
        round: Round,
        block: Option<Arc<Block>>,
    ) {
        if self.convergence.is_some() {
            Self::handle_fetched_parent(&mut self.convergence, digest, round, block);
        } else if let Some(block) = block {
            if block.digest() != digest {
                warn!("fetched convergence target has the wrong digest; discarding it");
            } else if block.height() > self.network_finalized_tip.1 {
                self.pending_head.height = Some(block.height());
                self.convergence = Some(PayloadWalk::converge(block));
            } else {
                self.pending_head = PendingHead::finalized(self.network_finalized_tip);
            }
        } else {
            warn!("marshal dropped the convergence target subscription; retrying the fetch");
        }
    }

    /// Picks the next execution task: consensus request, the forkchoice
    /// update forced by a long run of deliveries, finalized deliveries, the
    /// forkchoice update finalizing them, notarized deliveries, the
    /// forkchoice update moving the head. Finality goes
    /// first so a long notarized run never holds up acknowledgements;
    /// deliveries go before the update so one update covers a whole run.
    #[instrument(
        skip_all,
        fields(
            current.head_height = %self.local_state.head.0,
            current.head_digest = %self.local_state.head.1,
            current.finalized_height = %self.local_state.finalized.0,
            current.finalized_digest = %self.local_state.finalized.1,
        ),
        err,
    )]
    fn start_next_execution_task(&mut self) -> eyre::Result<()> {
        if !self.execution_task.is_none() {
            return Ok(());
        }

        // Continue a started walk before admitting another verification.
        // Fetches and retry delays leave the engine slot available to finality
        // and builds; support for multiple live walks is a separate change.
        if self
            .verification
            .as_ref()
            .is_some_and(|v| v.step == WalkStep::Probe)
        {
            let verification = self.verification.take().expect("verification is ready");
            let fut = execute_validation(self.execution_node.clone(), verification);
            self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Verify, fut));
            return Ok(());
        }

        // Latency critical requests come first: consensus is waiting on
        // them to vote on or propose a block.
        //
        // Probe candidates immediately: the execution layer may already know
        // their ancestry even when the actor has no record of it.
        match self.pending_consensus_request.take() {
            Some((round, ConsensusRequest::Verify(request))) => {
                if self.verification.is_none() {
                    let fut =
                        execute_validation(self.execution_node.clone(), PayloadWalk::new(request));
                    self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Verify, fut));
                    return Ok(());
                }
                self.pending_consensus_request = Some((round, ConsensusRequest::Verify(request)));
            }
            Some((round, ConsensusRequest::Build { cause, build })) => {
                // Admission selected this request's parent as the pending head
                // unless a newer context had already arrived. Keep the build
                // queued until convergence reaches it or the target changes.
                let pending_head = self.pending_head.digest;
                if build.context.parent.1 != pending_head {
                    info!(
                        %pending_head,
                        build.parent = %build.context.parent.1,
                        "build is not on the pending head, dropping it",
                    );
                } else if self.local_state.head.1 == pending_head {
                    let target = self.local_state;
                    let fut = execute_forkchoice(
                        self.execution_node.clone(),
                        cause.clone(),
                        target,
                        Some((cause, build)),
                    );
                    self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Build, fut));
                    return Ok(());
                } else {
                    // Fall through to convergence. The next body fetch,
                    // delivery, or retry heartbeat wakes the waiting build.
                    self.pending_consensus_request =
                        Some((round, ConsensusRequest::Build { cause, build }));
                }
            }
            None => {}
        }

        // Too many blocks delivered since the last forkchoice update: lock
        // them in before delivering more.
        if self.deliveries_since_forkchoice >= DELIVERIES_PER_FORKCHOICE_UPDATE
            && self.start_forkchoice_update()?
        {
            return Ok(());
        }

        // Finalizations are delivered in order and acknowledged so that the
        // marshal actor can make progress. Every finalized block is
        // delivered, whether or not the execution layer is thought to know
        // it: a known block is answered `VALID` from its caches without
        // being executed again.
        if let Some(request) = self.pending_finalizations.pop_front() {
            let fut = execute_finalization(self.execution_node.clone(), request);
            self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Finalize, fut));
            return Ok(());
        }

        // With the finalized queue drained, delivered finalized blocks that
        // await their acknowledgement are locked in before notarized
        // convergence continues. The update takes the head target that is
        // known at this point along.
        if !self.pending_acknowledgements.is_empty() && self.start_forkchoice_update()? {
            return Ok(());
        }

        if self
            .convergence
            .as_ref()
            .is_some_and(|walk| walk.step == WalkStep::Probe)
        {
            let walk = self.convergence.take().expect("convergence walk is ready");
            let fut = execute_validation(self.execution_node.clone(), walk);
            self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Deliver, fut));
            return Ok(());
        }

        self.start_forkchoice_update()?;
        Ok(())
    }

    /// Starts the forkchoice update onto the next target, if any; returns
    /// whether it did.
    fn start_forkchoice_update(&mut self) -> eyre::Result<bool> {
        // The counter is only cleared once an update is on its way: with no
        // target, the delivered blocks stay uncanonicalized and the debt
        // stands until the ancestry becomes walkable. Whatever the update
        // then does not cover cannot be canonicalized by any update.
        let Some(target) = self.next_forkchoice_target()? else {
            return Ok(false);
        };
        self.deliveries_since_forkchoice = 0;
        let fut = execute_forkchoice(self.execution_node.clone(), Span::current(), target, None);
        self.set_execution_task(ExecutionTask::new(ExecutionTaskType::Forkchoice, fut));
        Ok(true)
    }

    /// The next forkchoice state, if it differs from the tracked one: the
    /// delivered finalized block, and the selected parent once proven executed.
    /// When finality advances beneath the current head, check that head against
    /// the canonical chain and re-anchor if it does not descend from finality.
    fn next_forkchoice_target(&self) -> eyre::Result<Option<LocalState>> {
        let local = self.local_state;
        let (finalized_height, finalized_digest) = self.delivered_finalized;
        let mut target = local.update_finalized(finalized_height, finalized_digest);
        if let Some(height) = self.known_height(self.pending_head.digest) {
            target = target.update_head(height, self.pending_head.digest);
        }

        if target.finalized != local.finalized && target.head == local.head {
            let canonical_hash = self
                .execution_node
                .canonical_block_hash(finalized_height.get())
                .wrap_err_with(|| {
                    format!(
                        "failed reading canonical execution block hash at finalized \
                        block height `{finalized_height}`",
                    )
                })?;
            let head_descends_from_finalized = canonical_hash == Some(finalized_digest.0);
            if !head_descends_from_finalized {
                target = target.update_head(finalized_height, finalized_digest);
            }
        }

        Ok((target != local).then_some(target))
    }
}

#[instrument(skip_all, fields(height), err)]
async fn get_block(
    marshal: impl Marshal,
    execution_node: impl ExecutionLayer,
    height: Height,
) -> eyre::Result<Block> {
    if let Some(block) = marshal.get_block(height).await {
        return Ok(block);
    }

    warn!(
        "marshal did not have backfill block; looking up its finalized digest \
        to look for it in the execution layer"
    );
    let Some((_, digest)) = marshal.get_info(height).await else {
        bail!("marshal actor did not have finalization info at height");
    };

    info!(
        %digest,
        "found finalized digest for block height; checking execution layer",
    );
    let Some(block) = execution_node.block_by_digest(digest).wrap_err_with(|| {
        format!("failed querying execution layer for backfill block `{digest}`")
    })?
    else {
        warn!(%digest, "execution layer did not have missing backfill block");
        bail!(
            "marshal actor did not have block at height `{height}` and \
            execution layer did not have block `{digest}`"
        );
    };

    Ok(block)
}

struct FinalizedBlockRequest {
    cause: Span,
    block: Arc<Block>,
    acknowledgment: Exact,
}

/// An in-flight body fetch, keyed by digest and the round it was notarized in.
///
/// Resolves to the digest, the round, and the fetched block - `None` for the
/// block if the marshal actor dropped the channel before delivering it.
struct PendingNotarizedBlock {
    digest: Digest,
    round: Round,
    fetch: tokio::sync::oneshot::Receiver<Arc<Block>>,
}

impl PendingNotarizedBlock {
    fn new(marshal: &impl Marshal, round: Round, digest: Digest) -> Self {
        let fetch = marshal.subscribe_by_digest(digest, round);
        Self {
            digest,
            round,
            fetch,
        }
    }
}

impl Future for PendingNotarizedBlock {
    type Output = (Digest, Round, Option<Arc<Block>>);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let block = std::task::ready!(self.fetch.poll_unpin(cx));
        std::task::Poll::Ready((self.digest, self.round, block.ok()))
    }
}

/// A latency-critical request from a consensus round: the node is either
/// asked to validate the round's proposal or to build it.
enum ConsensusRequest {
    Verify(PayloadRequest),
    Build { cause: Span, build: Box<Build> },
}

/// Queues `request` into `slot` unless the slot already holds a request from
/// the same or a newer round.
///
/// Propose and verify are mutually exclusive within a round. Because Simplex
/// views are strictly monotonically increasing, a request at or below the
/// queued round cannot represent later consensus progress and must not replace
/// the request already queued. The application's handlers run concurrently,
/// so a request sent by a dying older-round task can still arrive after a newer
/// one; the round guard keeps it from clobbering the newer request. Dropping a
/// request - superseded or stale - drops its response channel, signalling the
/// failure to the subscriber.
fn queue_consensus_request(
    slot: &mut Option<(Round, ConsensusRequest)>,
    round: Round,
    request: ConsensusRequest,
) {
    match slot {
        Some((queued, _)) if round <= *queued => {
            debug!(
                %round,
                queued_round = %queued,
                "dropping consensus request at or below the queued round",
            );
        }
        Some(_) => {
            debug!(%round, "consensus request superseded a queued one");
            *slot = Some((round, request));
        }
        None => *slot = Some((round, request)),
    }
}

/// The root of an EL-driven walk: a verification candidate or a build's parent.
struct PayloadRequest {
    parent_round: Round,
    cause: Span,
    block: Arc<Block>,
    validator_set: Option<Vec<B256>>,
    /// Delivers the validation result: `Some(duration)` when the execution
    /// layer accepted the block, `None` when it rejected it. Dropped without
    /// a value when validation was not possible or the request was
    /// superseded. Absent for build-parent delivery, which has no verdict subscriber.
    response: Option<oneshot::Sender<Option<Duration>>>,
}

impl PayloadRequest {
    fn is_canceled(&self) -> bool {
        self.response
            .as_ref()
            .is_some_and(oneshot::Sender::is_canceled)
    }

    async fn cancellation(&mut self) {
        match self.response.as_mut() {
            Some(response) => response.cancellation().await,
            None => std::future::pending().await,
        }
    }
}

/// State retained between engine calls, shared by verification and build-parent delivery.
struct PayloadWalk {
    request: PayloadRequest,
    /// The candidate or ancestor currently being probed.
    cursor: Arc<Block>,
    step: WalkStep,
    /// Engine call time, excluding time waiting for ancestor bodies or retries.
    duration: Duration,
    /// The candidate has been re-probed after an ancestor's terminal answer.
    /// If it still returns SYNCING, pause before starting another walk.
    reprobed: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WalkStep {
    Probe,
    Parent,
    Retry,
}

impl PayloadWalk {
    fn converge(block: Arc<Block>) -> Self {
        let context = block.context();
        Self::new(PayloadRequest {
            parent_round: Round::new(context.round.epoch(), context.parent.0),
            cause: Span::current(),
            block,
            validator_set: None,
            response: None,
        })
    }

    fn retry(&mut self) {
        self.reprobe();
        self.reprobed = false;
    }

    fn new(request: PayloadRequest) -> Self {
        Self {
            cursor: request.block.clone(),
            request,
            step: WalkStep::Probe,
            duration: Duration::ZERO,
            reprobed: false,
        }
    }

    fn is_candidate(&self) -> bool {
        self.cursor.digest() == self.request.block.digest()
    }

    fn parent(&self) -> (Round, Digest) {
        let round = if self.is_candidate() {
            self.request.parent_round
        } else {
            let context = self.cursor.context();
            Round::new(context.round.epoch(), context.parent.0)
        };
        (round, self.cursor.parent_digest())
    }

    fn reprobe(&mut self) {
        self.cursor = self.request.block.clone();
        self.step = WalkStep::Probe;
        self.reprobed = true;
    }
}

#[derive(Debug, Clone, Copy)]
enum ExecutionTaskType {
    Heartbeat,
    Verify,
    Build,
    Deliver,
    Finalize,
    Forkchoice,
}

impl ExecutionTaskType {
    fn name(self) -> &'static str {
        match self {
            Self::Heartbeat => "heartbeat",
            Self::Verify => "verify",
            Self::Build => "build",
            Self::Deliver => "deliver",
            Self::Finalize => "finalize",
            Self::Forkchoice => "forkchoice",
        }
    }
}

struct ExecutionTask {
    task_type: ExecutionTaskType,
    span: Span,
    started_at: Instant,
    fut: BoxFuture<'static, ExecutionTaskOutcome>,
}

impl ExecutionTask {
    fn new<F>(task_type: ExecutionTaskType, fut: F) -> Self
    where
        F: Future<Output = ExecutionTaskOutcome> + Send + 'static,
    {
        Self {
            task_type,
            span: Span::none(),
            started_at: Instant::now(),
            fut: fut.boxed(),
        }
    }
}

struct ExecutionTaskFinished {
    task_type: ExecutionTaskType,
    span: Span,
    started_at: Instant,
    outcome: ExecutionTaskOutcome,
}

impl ExecutionTaskFinished {
    fn target(&self) -> Option<LocalState> {
        match &self.outcome {
            ExecutionTaskOutcome::Forkchoice { target, .. } => Some(*target),
            ExecutionTaskOutcome::Validated { .. }
            | ExecutionTaskOutcome::FinalizedDelivered { .. } => None,
        }
    }
}

impl Future for ExecutionTask {
    type Output = ExecutionTaskFinished;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let span = self.span.clone();
        let outcome = {
            let _entered = span.enter();
            std::task::ready!(self.fut.as_mut().poll(cx))
        };
        std::task::Poll::Ready(ExecutionTaskFinished {
            task_type: self.task_type,
            span,
            started_at: self.started_at,
            outcome,
        })
    }
}

/// What an execution task got back from its single engine call, reported
/// uninterpreted; [`Actor::handle_execution_task_finished`] decides.
enum ExecutionTaskOutcome {
    /// The request travels back for its verdict; `None` if the subscriber
    /// went away meanwhile. The status comes with the time taken.
    Validated {
        request: Option<PayloadWalk>,
        status: eyre::Result<(PayloadStatusEnum, Duration)>,
    },
    /// The request travels back for its acknowledgement.
    FinalizedDelivered {
        request: FinalizedBlockRequest,
        status: eyre::Result<PayloadStatusEnum>,
    },
    /// The raw answer to a forkchoice update, together with the build
    /// subscriber it carried; `None` if the update was stale and not
    /// submitted.
    Forkchoice {
        target: LocalState,
        build: Option<(Span, oneshot::Sender<TempoBuiltPayload>)>,
        response: Option<eyre::Result<ForkchoiceUpdated>>,
    },
}

impl ExecutionTaskOutcome {
    fn name(&self) -> &'static str {
        match self {
            Self::Validated { .. } => "validated",
            Self::FinalizedDelivered { .. } => "finalized-delivered",
            Self::Forkchoice { .. } => "forkchoice",
        }
    }
}

/// A payload build registered on the execution layer whose result still needs
/// to be delivered to the subscriber that requested it.
struct StartPayloadJob {
    cause: Span,
    payload_id: PayloadId,
    response: oneshot::Sender<TempoBuiltPayload>,
}

/// Submits a forkchoice update targeting `target`, with the build's payload
/// attributes if the build is still wanted. A no-op update is submitted
/// regardless (heartbeats rely on this).
#[instrument(
    skip_all,
    parent = &cause,
    fields(
        head_block_hash = %target.head.1,
        head_block_height = %target.head.0,
        finalized_block_hash = %target.finalized.1,
        finalized_block_height = %target.finalized.0,
        build = build.is_some(),
    ),
)]
async fn execute_forkchoice(
    execution_node: impl ExecutionLayer,
    cause: Span,
    target: LocalState,
    build: Option<(Span, Box<Build>)>,
) -> ExecutionTaskOutcome {
    let build = build.filter(|(_, build)| {
        if build.response.is_canceled() {
            info!(
                "dropping payload build request: subscriber went away while \
                awaiting execution"
            );
            return false;
        }
        true
    });
    let (build, attributes) = match build {
        Some((cause, build)) => {
            let Build {
                attributes,
                response,
                ..
            } = *build;
            (Some((cause, response)), Some(*attributes))
        }
        None => (None, None),
    };

    let response = submit_forkchoice_update(&execution_node, cause, target, attributes).await;
    ExecutionTaskOutcome::Forkchoice {
        target,
        build,
        response,
    }
}

/// Delivers a finalized block through a bare new-payload request.
#[instrument(
    skip_all,
    parent = &request.cause,
    fields(
        block.digest = %request.block.digest(),
        block.height = %request.block.height(),
    ),
)]
async fn execute_finalization(
    execution_node: impl ExecutionLayer,
    request: FinalizedBlockRequest,
) -> ExecutionTaskOutcome {
    // The validator set can be omitted for finalized blocks.
    let status = deliver_block(&execution_node, request.block.clone(), None).await;
    ExecutionTaskOutcome::FinalizedDelivered { request, status }
}

/// Probes one candidate or ancestor and returns the walk with the answer. A
/// canceled subscriber abandons the walk and its ancestor fetch.
#[instrument(
    skip_all,
    parent = &verification.request.cause,
    fields(
        block.digest = %verification.cursor.digest(),
        block.height = %verification.cursor.height(),
        block.parent_digest = %verification.cursor.parent_digest(),
    ),
)]
async fn execute_validation(
    execution_node: impl ExecutionLayer,
    mut verification: PayloadWalk,
) -> ExecutionTaskOutcome {
    let validation_start = Instant::now();
    let work = deliver_block(
        &execution_node,
        verification.cursor.clone(),
        if verification.is_candidate() {
            verification.request.validator_set.clone()
        } else {
            None
        },
    );
    futures::pin_mut!(work);

    let status = select! {
        biased;

        status = &mut work => status
            .map(|status| (status, validation_start.elapsed()))
            .wrap_err("failed sending new-payload request to execution layer to validate block"),

        // Stops waiting for the verdict; the execution layer may still
        // process the new-payload request. Cancellation abandons this walk.
        () = verification.request.cancellation() => {
            info!(
                "verification subscriber went away before the block was \
                validated; abandoning the request"
            );
            return ExecutionTaskOutcome::Validated {
                request: None,
                status: Err(eyre!("verification subscriber went away")),
            };
        }
    };

    ExecutionTaskOutcome::Validated {
        request: Some(verification),
        status,
    }
}

/// Submits `block` to the execution layer through a new-payload request and
/// returns the reported payload status.
async fn deliver_block(
    execution_node: &impl ExecutionLayer,
    block: Arc<Block>,
    validator_set: Option<Vec<B256>>,
) -> eyre::Result<PayloadStatusEnum> {
    let (block, block_access_list) = Arc::unwrap_or_clone(block).into_parts();
    let payload_status = execution_node
        .new_payload(TempoExecutionData {
            block,
            block_access_list,
            validator_set,
        })
        .await
        .wrap_err("failed sending new-payload request to execution layer")?;
    if payload_status.is_valid() {
        info!(%payload_status, "execution layer reported payload status");
    } else {
        warn!(%payload_status, "execution layer reported payload status");
    }
    Ok(payload_status.status)
}

/// Whether `target` finalizes below the execution layer's own finality, so
/// that submitting it would move finality backwards. The tracked state
/// trails execution-layer finality after a snapshot restore until the
/// marshal actor's re-deliveries catch up; a tracked finalized block the
/// execution layer's canonical chain contradicts is fatal.
fn is_stale_forkchoice(
    execution_node: &impl ExecutionLayer,
    target: LocalState,
) -> eyre::Result<bool> {
    let execution_finalized = execution_node.finalized_num_hash();
    if execution_finalized.number < target.finalized.0.get() {
        return Ok(false);
    }
    let canonical_digest = execution_node
        .canonical_block_hash(target.finalized.0.get())
        .wrap_err_with(|| {
            format!(
                "failed reading canonical execution block hash at the tracked \
                finalized height `{}`",
                target.finalized.0,
            )
        })?
        .ok_or_else(|| {
            eyre!(
                "no canonical execution block hash at the tracked finalized height \
                `{}`, even though it is at or below the execution layer's finalized \
                height `{}`",
                target.finalized.0,
                execution_finalized.number,
            )
        })?;
    ensure!(
        canonical_digest == target.finalized.1.0,
        "tracked finalized block `{}` at height `{}` conflicts with the execution \
        layer's canonical block `{canonical_digest}` at the same height, which the \
        execution layer already considers final; two different blocks must never be \
        finalized at the same height",
        target.finalized.1,
        target.finalized.0,
    );
    if execution_finalized.number > target.finalized.0.get() {
        debug!(
            execution_finalized_height = execution_finalized.number,
            execution_finalized_hash = %execution_finalized.hash,
            "tracked finalized state is below the execution layer's finalized tip; \
            skipping the forkchoice update",
        );
        return Ok(true);
    }
    Ok(false)
}

/// Drives a payload build on the execution layer to completion.
///
/// Resolves the payload registered under `payload_id` from the execution
/// layer's payload builder and delivers it on `response`. If the subscriber
/// goes away before the payload is resolved (for example because the
/// consensus engine cancelled the proposal request that triggered the
/// build), the in-flight resolve future is dropped, which deregisters the
/// build job from the payload builder and aborts the build.
#[instrument(
    skip_all,
    parent = &cause,
    fields(%payload_id),
)]
async fn run_payload_job(
    execution_node: impl ExecutionLayer,
    StartPayloadJob {
        cause,
        payload_id,
        mut response,
    }: StartPayloadJob,
) -> Option<Arc<Block>> {
    let payload = select! {
        payload = execution_node
            .resolve_payload(payload_id)
        => payload,

        // Drops the in-flight payload-resolution, killing payload build.
        () = response.cancellation() => {
            info!("payload subscriber went away before the payload was resolved; killing the payload build");
            return None;
        }
    };

    // In the failure branches, dropping the response channel signals the
    // failure to the subscriber; the cause is only logged here.
    match payload {
        Some(Ok(payload)) => {
            let retained = payload.clone();
            if response.send(payload).is_err() {
                info!(
                    "payload subscriber went away before the payload could be delivered; discarding it"
                );
                return None;
            }
            // The application received the block and may propose it; hand
            // the body to the actor loop for a later build on this proposal.
            let (execution_block, block_access_list, _) =
                retained.into_consensus_execution_payload();
            Some(Arc::new(Block::from_execution_block_unchecked(
                execution_block,
                block_access_list,
            )))
        }
        Some(Err(error)) => {
            warn!(
                %error,
                "payload build job failed",
            );
            None
        }
        None => {
            warn!("no payload build job found under the payload ID");
            None
        }
    }
}

/// Submits the forkchoice update unless it is stale (see
/// [`is_stale_forkchoice`]), in which case nothing is sent and `None` is
/// returned. A failing stale check is reported like a failed update; the
/// response is returned raw.
#[instrument(
    skip_all,
    parent = &cause,
    fields(
        head_block_hash = %canonicalized.head.1,
        head_block_height = %canonicalized.head.0,
        finalized_block_hash = %canonicalized.finalized.1,
        finalized_block_height = %canonicalized.finalized.0,
    ),
)]
async fn submit_forkchoice_update(
    execution_node: &impl ExecutionLayer,
    cause: Span,
    canonicalized: LocalState,
    attrs: Option<TempoPayloadAttributes>,
) -> Option<eyre::Result<ForkchoiceUpdated>> {
    match is_stale_forkchoice(execution_node, canonicalized) {
        Ok(false) => {}
        Ok(true) => return None,
        Err(error) => return Some(Err(error)),
    }

    let fcu_response = match execution_node
        .fork_choice_updated(canonicalized.to_forkchoice_state(), attrs)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return Some(Err(error.wrap_err(
                "failed requesting execution layer to update forkchoice state",
            )));
        }
    };
    if fcu_response.is_invalid() {
        warn!(
            payload_status = %fcu_response.payload_status,
            "execution layer reported FCU status",
        );
    } else {
        info!(
            payload_status = %fcu_response.payload_status,
            "execution layer reported FCU status",
        );
    }
    Some(Ok(fcu_response))
}

/// A snapshot of the execution layer's local state - its head and
/// finalized tip - for execution tasks to extend and report back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalState {
    head: (Height, Digest),
    finalized: (Height, Digest),
}

impl LocalState {
    /// Transform a [`LocalState`] to a [`ForkchoiceState`] to submit to the
    /// execution layer.
    fn to_forkchoice_state(self) -> ForkchoiceState {
        ForkchoiceState {
            head_block_hash: self.head.1.0,
            safe_block_hash: self.finalized.1.0,
            finalized_block_hash: self.finalized.1.0,
        }
    }

    /// Updates the finalized tip to `digest` at `height`.
    ///
    /// `height` must be ahead of the tracked finalized height; if it is
    /// not, this is a no-op. If `height` is at or ahead of the head
    /// height, the head is moved onto the finalized tip as well, so that
    /// the finalized tip is never ahead of the head.
    fn update_finalized(self, height: Height, digest: Digest) -> Self {
        let mut this = self;
        if height > this.finalized.0 {
            this.finalized = (height, digest);
        }
        if height >= this.head.0 {
            this.head = (height, digest);
        }
        this
    }

    /// Updates the head to `digest` at `height`.
    ///
    /// The head only moves above the finalized tip (or back onto it);
    /// anything below is a no-op.
    fn update_head(self, height: Height, digest: Digest) -> Self {
        let mut this = self;
        if height > this.finalized.0 || digest == this.finalized.1 {
            this.head = (height, digest);
        }
        this
    }
}

struct PendingHead {
    round: Round,
    digest: Digest,
    height: Option<Height>,
    /// Builds must deliver their parent; verification supplies its own walk.
    requires_delivery: bool,
}

impl PendingHead {
    fn finalized((round, height, digest): (Round, Height, Digest)) -> Self {
        Self {
            round,
            digest,
            height: Some(height),
            requires_delivery: false,
        }
    }
}

fn update_block_fetch(
    marshal: &impl Marshal,
    pending: &mut OptionFuture<PendingNotarizedBlock>,
    next: Option<(Round, Digest)>,
) {
    if pending
        .as_ref()
        .is_some_and(|pending| next.map(|(_, digest)| digest) != Some(pending.digest))
    {
        *pending = OptionFuture::none();
    }
    if pending.is_none()
        && let Some((round, digest)) = next
    {
        pending.replace(PendingNotarizedBlock::new(marshal, round, digest));
    }
}

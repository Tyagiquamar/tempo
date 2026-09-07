//! Verification retains the candidate and discovers ancestors from SYNCING.

use std::time::Duration;

use alloy_rpc_types_engine::PayloadStatusEnum;
use commonware_macros::test_traced;
use commonware_runtime::{Runner as _, deterministic};

use super::harness::{FakeExecution, GENESIS, Harness, built_payload, make_block, round};

#[test_traced]
fn valid_candidate_converges_to_its_parent_without_fetching_or_delivering_it() {
    deterministic::Runner::default().start(|context| async move {
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let execution = FakeExecution::new();
        execution.seed_canonical_block(&parent);
        let h = Harness::builder().execution(execution).start(&context);

        h.verify(round(2), candidate.clone())
            .await
            .unwrap()
            .unwrap();
        h.wait_until(|| h.execution.head() == parent.digest()).await;
        h.run_for(Duration::from_millis(50)).await;
        assert_eq!(h.execution.new_payloads(), vec![candidate.digest()]);
        assert!(h.marshal.subscribe_log().is_empty());
        assert!(
            h.execution
                .fcus()
                .iter()
                .all(|(head, _, _)| *head != candidate.digest())
        );
    });
}

#[test_traced]
fn canceling_verification_does_not_start_a_background_ancestry_walk() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let mut verify = Box::pin(h.verify(round(2), candidate.clone()));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.open_subscriptions() == vec![(parent.digest(), round(1))])
            .await;
        drop(verify);
        h.wait_until(|| h.marshal.open_subscriptions().is_empty())
            .await;
        h.run_for(Duration::from_secs(2)).await;
        assert_eq!(h.execution.new_payloads(), vec![candidate.digest()]);
        assert_eq!(h.marshal.subscribe_log(), vec![(parent.digest(), round(1))]);

        h.build(round(1), GENESIS)
            .await
            .expect_err("cancellation must preserve the newer target");
    });
}

#[test_traced]
fn a_waiting_walk_leaves_ready_build_convergence_on_another_branch_runnable() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let other_parent = make_block(1, 1, GENESIS);
        let other_digest = other_parent.digest();
        h.execution
            .script_built_payload(built_payload(&other_parent));
        h.build(round(1), GENESIS).await.unwrap();

        let missing = make_block(2, 1, GENESIS);
        let candidate = make_block(3, 2, missing.digest());
        let mut verify = Box::pin(h.verify(round(3), candidate));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.open_subscriptions() == vec![(missing.digest(), round(2))])
            .await;

        let proposal = make_block(4, 2, other_digest);
        h.execution.script_built_payload(built_payload(&proposal));
        let mut build = h.build_on(round(4), 1, other_digest);
        h.wait_until(|| h.execution.head() == other_digest).await;
        h.wait_until(|| h.execution.fcus().contains(&(other_digest, GENESIS, true)))
            .await;
        assert!(build.try_recv().unwrap().is_some());
        assert!(futures::poll!(&mut verify).is_pending());
        assert_eq!(
            h.marshal.open_subscriptions(),
            vec![(missing.digest(), round(2))]
        );
    });
}

#[test_traced]
fn verification_and_build_parent_fetches_make_progress_independently() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let mut verify = Box::pin(h.verify(round(2), candidate));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.open_subscriptions() == vec![(parent.digest(), round(1))])
            .await;

        let other = make_block(3, 1, GENESIS);
        let proposal = make_block(4, 2, other.digest());
        h.execution.script_built_payload(built_payload(&proposal));
        let build = h.build(round(4), other.digest());
        h.wait_until(|| h.marshal.open_subscriptions().len() == 2)
            .await;
        assert!(
            h.marshal
                .fulfill_subscription(other.digest(), other.clone())
        );
        build.await.unwrap();
        assert!(futures::poll!(&mut verify).is_pending());

        assert!(h.marshal.fulfill_subscription(parent.digest(), parent));
        verify.await.unwrap().unwrap();
        h.run_for(Duration::from_millis(20)).await;
        assert_eq!(
            h.execution.head(),
            other.digest(),
            "the older verification cannot restore its parent target"
        );
    });
}

#[test_traced]
fn verification_retries_do_not_wake_a_rejected_build_parent_early() {
    deterministic::Runner::default().start(|context| async move {
        let mut h = Harness::start_at_genesis(&context);
        let finalized = make_block(1, 1, GENESIS);
        h.deliver_tip(round(1), 1, finalized.digest());
        let candidate = make_block(2, 2, finalized.digest());
        let mut verify = Box::pin(h.verify(round(2), candidate));
        assert!(futures::poll!(&mut verify).is_pending());

        let parent = make_block(3, 2, finalized.digest());
        let digest = parent.digest();
        h.execution.script_new_payload(
            digest,
            Ok(PayloadStatusEnum::Invalid {
                validation_error: "retry later".into(),
            }),
        );
        h.execution
            .script_new_payload(digest, Ok(PayloadStatusEnum::Valid));
        let proposal = make_block(4, 3, digest);
        h.execution.script_built_payload(built_payload(&proposal));
        let build = h.build(round(4), digest);
        h.wait_until(|| h.marshal.fulfill_subscription(digest, parent.clone()))
            .await;
        h.wait_until(|| h.execution.new_payloads().contains(&digest))
            .await;
        h.run_for(Duration::from_secs(2)).await;
        assert_eq!(
            h.execution
                .new_payloads()
                .iter()
                .filter(|d| **d == digest)
                .count(),
            1
        );

        h.deliver_finalized(finalized).await.unwrap();
        verify.await.unwrap().unwrap();
        build.await.unwrap();
        assert_eq!(h.execution.head(), digest);
    });
}

#[test_traced]
fn syncing_walks_backward_one_response_at_a_time_then_reprobes_the_candidate() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let b1 = make_block(1, 1, GENESIS);
        let b2 = make_block(2, 2, b1.digest());
        let b3 = make_block(3, 3, b2.digest());
        let (d1, d2, d3) = (b1.digest(), b2.digest(), b3.digest());
        let release = h
            .execution
            .script_delayed_new_payload(d2, Ok(PayloadStatusEnum::Syncing));

        let mut verify = Box::pin(h.verify(round(3), b3));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.open_subscriptions() == vec![(d2, round(2))])
            .await;
        assert_eq!(h.execution.new_payloads(), vec![d3]);
        assert!(h.marshal.fulfill_subscription(d2, b2));
        h.wait_until(|| h.execution.new_payloads() == vec![d3, d2])
            .await;
        assert!(
            h.marshal.open_subscriptions().is_empty(),
            "no ancestor fetch before the engine answers"
        );

        release.send(()).unwrap();
        h.wait_until(|| h.marshal.open_subscriptions() == vec![(d1, round(1))])
            .await;
        assert!(h.marshal.fulfill_subscription(d1, b1));
        assert!(verify.await.unwrap().is_some());
        assert_eq!(h.execution.new_payloads(), vec![d3, d2, d1, d3]);
        assert_eq!(
            h.marshal.subscribe_log(),
            vec![(d2, round(2)), (d1, round(1))]
        );
        h.wait_until(|| h.execution.head() == d2).await;
        assert!(h.execution.fcus().iter().all(|(head, _, _)| *head != d3));
    });
}

#[test_traced]
fn valid_ancestor_stops_the_walk_above_the_finalized_tip() {
    deterministic::Runner::default().start(|context| async move {
        let b1 = make_block(1, 1, GENESIS);
        let b2 = make_block(2, 2, b1.digest());
        let b3 = make_block(3, 3, b2.digest());
        let (d2, d3) = (b2.digest(), b3.digest());
        let execution = FakeExecution::new();
        execution.seed_canonical_block(&b1);
        let h = Harness::builder().execution(execution).start(&context);

        let mut verify = Box::pin(h.verify(round(3), b3));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.fulfill_subscription(d2, b2.clone()))
            .await;
        assert!(verify.await.unwrap().is_some());
        h.wait_until(|| h.execution.head() == d2).await;
        assert_eq!(h.execution.new_payloads(), vec![d3, d2, d3]);
        assert_eq!(
            h.marshal.subscribe_log(),
            vec![(d2, round(2))],
            "VALID b2 makes fetching b1 unnecessary"
        );
    });
}

#[test_traced]
fn syncing_fetches_the_parent_even_if_a_previous_verification_said_valid() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let b1 = make_block(1, 1, GENESIS);
        let d1 = b1.digest();
        h.verify(round(1), b1.clone()).await.unwrap().unwrap();
        let b2 = make_block(2, 2, d1);
        let d2 = b2.digest();
        h.execution
            .script_new_payload(d2, Ok(PayloadStatusEnum::Syncing));
        h.execution
            .script_new_payload(d2, Ok(PayloadStatusEnum::Valid));
        let mut verify = Box::pin(h.verify(round(2), b2));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.fulfill_subscription(d1, b1.clone()))
            .await;
        verify.await.unwrap().unwrap();
        assert_eq!(h.execution.new_payloads(), vec![d1, d2, d1, d2]);
        assert_eq!(h.marshal.subscribe_log(), vec![(d1, round(1))]);
    });
}

#[test_traced]
fn an_ancestors_verdict_does_not_replace_the_candidates_verdict() {
    for ancestor_status in [
        PayloadStatusEnum::Valid,
        PayloadStatusEnum::Invalid {
            validation_error: "bad ancestor".into(),
        },
    ] {
        deterministic::Runner::default().start(|context| async move {
            let h = Harness::start_at_genesis(&context);
            let parent = make_block(1, 1, GENESIS);
            let candidate = make_block(2, 2, parent.digest());
            let (parent_digest, candidate_digest) = (parent.digest(), candidate.digest());
            h.execution
                .script_new_payload(candidate_digest, Ok(PayloadStatusEnum::Syncing));
            h.execution.script_new_payload(
                candidate_digest,
                Ok(PayloadStatusEnum::Invalid {
                    validation_error: "invalid candidate".into(),
                }),
            );
            h.execution
                .script_new_payload(parent_digest, Ok(ancestor_status));
            let mut verify = Box::pin(h.verify(round(2), candidate));
            assert!(futures::poll!(&mut verify).is_pending());
            h.wait_until(|| {
                h.marshal
                    .fulfill_subscription(parent_digest, parent.clone())
            })
            .await;
            assert!(verify.await.unwrap().is_none());
            assert_eq!(
                h.execution.new_payloads(),
                vec![candidate_digest, parent_digest, candidate_digest]
            );
            assert!(
                h.execution
                    .fcus()
                    .iter()
                    .all(|(head, _, _)| *head != candidate_digest)
            );
        });
    }
}

#[test_traced]
fn still_syncing_after_a_valid_ancestor_retries_without_spinning() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let (p, c) = (parent.digest(), candidate.digest());
        for status in [
            PayloadStatusEnum::Syncing,
            PayloadStatusEnum::Syncing,
            PayloadStatusEnum::Valid,
        ] {
            h.execution.script_new_payload(c, Ok(status));
        }
        let mut verify = Box::pin(h.verify(round(2), candidate));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.fulfill_subscription(p, parent.clone()))
            .await;
        h.wait_until(|| h.execution.new_payloads() == vec![c, p, c])
            .await;
        h.run_for(Duration::from_millis(500)).await;
        assert_eq!(h.execution.new_payloads(), vec![c, p, c]);
        assert!(futures::poll!(&mut verify).is_pending());
        h.run_for(Duration::from_millis(600)).await;
        assert!(verify.await.unwrap().is_some());
        assert_eq!(h.execution.new_payloads(), vec![c, p, c, c]);
    });
}

#[test_traced]
fn advancing_finality_cancels_the_fetch_and_resumes_verification_after_delivery() {
    deterministic::Runner::default().start(|context| async move {
        let mut h = Harness::start_at_genesis(&context);
        let b1 = make_block(1, 1, GENESIS);
        let b2 = make_block(2, 2, b1.digest());
        let b3 = make_block(3, 3, b2.digest());
        let (d1, d2, d3) = (b1.digest(), b2.digest(), b3.digest());
        let mut verify = Box::pin(h.verify(round(3), b3));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.open_subscriptions() == vec![(d2, round(2))])
            .await;

        h.deliver_tip(round(2), 2, d2);
        h.wait_until(|| h.marshal.open_subscriptions().is_empty())
            .await;
        h.deliver_finalized(b1).await.unwrap();
        assert!(futures::poll!(&mut verify).is_pending());
        h.deliver_finalized(b2).await.unwrap();
        verify.await.unwrap().unwrap();
        assert_eq!(h.marshal.subscribe_log(), vec![(d2, round(2))]);
        assert_eq!(
            h.execution
                .new_payloads()
                .iter()
                .filter(|d| **d == d1 || **d == d2)
                .copied()
                .collect::<Vec<_>>(),
            vec![d1, d2],
            "only finalization delivers blocks below the moving boundary"
        );
        assert!(h.execution.fcus().iter().all(|(head, _, _)| *head != d3));
    });
}

#[test_traced]
fn canceling_a_walk_closes_an_obsolete_fetch_and_stops_retries() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let c = candidate.digest();
        let mut verify = Box::pin(h.verify(round(2), candidate));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| !h.marshal.open_subscriptions().is_empty())
            .await;
        let proposal = make_block(3, 1, GENESIS);
        h.execution.script_built_payload(built_payload(&proposal));
        let build = h.build(round(3), GENESIS);
        build.await.expect("newer build should complete on genesis");
        assert!(futures::poll!(&mut verify).is_pending());
        assert_eq!(
            h.marshal.open_subscriptions(),
            vec![(parent.digest(), round(1))],
            "the older walk still needs its ancestor until its subscriber cancels",
        );
        drop(verify);
        h.wait_until(|| h.marshal.open_subscriptions().is_empty())
            .await;
        h.run_for(Duration::from_secs(2)).await;
        assert_eq!(h.execution.new_payloads(), vec![c]);
    });
}

#[test_traced]
fn dropped_ancestor_fetch_is_reissued_while_the_candidate_stays_pending() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let p = parent.digest();
        let mut verify = Box::pin(h.verify(round(2), candidate));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.drop_subscription(p)).await;
        h.wait_until(|| h.marshal.subscribe_log().len() == 2).await;
        assert!(futures::poll!(&mut verify).is_pending());
        assert!(h.marshal.fulfill_subscription(p, parent));
        verify.await.unwrap().unwrap();
    });
}

#[test_traced]
fn an_ancestor_transport_error_fails_only_the_verification() {
    deterministic::Runner::default().start(|context| async move {
        let h = Harness::start_at_genesis(&context);
        let parent = make_block(1, 1, GENESIS);
        let candidate = make_block(2, 2, parent.digest());
        let p = parent.digest();
        h.execution.script_new_payload(p, Err("transport error"));
        let mut verify = Box::pin(h.verify(round(2), candidate));
        assert!(futures::poll!(&mut verify).is_pending());
        h.wait_until(|| h.marshal.fulfill_subscription(p, parent.clone()))
            .await;
        let _ = verify
            .await
            .expect_err("transport failure must release the request");
        let sibling = make_block(3, 1, GENESIS);
        h.verify(round(3), sibling).await.unwrap().unwrap();
    });
}

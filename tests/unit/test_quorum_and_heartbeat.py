"""Testes unitários de Replicação por Quorum, Heartbeat, FaultInjector e Métricas."""

from __future__ import annotations

import pytest

from mc714_p2.domain.fault_injector import FaultInjector
from mc714_p2.domain.heartbeat import HeartbeatFailureDetector, PeerHealthStatus
from mc714_p2.domain.metrics import (
    QuorumOperationRecord,
    summarize_latencies,
    verify_quorum_consistency,
)
from mc714_p2.domain.quorum import (
    EMPTY_VALUE,
    LocalReplicaStore,
    QuorumConfig,
    VersionedValue,
    compute_next_version,
    find_stale_replicas,
    select_highest_version,
)


def test_quorum_config_gifford_invariants() -> None:
    # Válidos para N=5: (3, 3), (2, 4), (1, 5)
    cfg = QuorumConfig(total_nodes=5, read_quorum=2, write_quorum=4)
    assert cfg.total_nodes == 5

    # Inválido: N < 3
    with pytest.raises(ValueError, match="mínimo 3 nós"):
        QuorumConfig(total_nodes=2, read_quorum=2, write_quorum=2)

    # Inválido: N_R + N_W <= N
    with pytest.raises(ValueError, match="interseção leitura-escrita"):
        QuorumConfig(total_nodes=5, read_quorum=2, write_quorum=3)

    # Inválido: N_W <= N/2
    with pytest.raises(ValueError, match="interseção escrita-escrita"):
        QuorumConfig(total_nodes=5, read_quorum=4, write_quorum=2)


def test_replica_store_and_version_selection() -> None:
    store = LocalReplicaStore()
    assert store.get("k1") == EMPTY_VALUE

    v1 = VersionedValue(value="a", version=1, writer_id=1, lamport_ts=10)
    v2_stale = VersionedValue(value="old", version=1, writer_id=0, lamport_ts=9)
    v2_newer = VersionedValue(value="b", version=2, writer_id=1, lamport_ts=12)

    applied, cur = store.put_if_newer("k1", v1)
    assert applied and cur == v1

    applied_stale, cur = store.put_if_newer("k1", v2_stale)
    assert not applied_stale and cur == v1

    applied_newer, cur = store.put_if_newer("k1", v2_newer)
    assert applied_newer and cur == v2_newer

    responses = {1: v1, 2: v2_newer, 3: EMPTY_VALUE}
    latest = select_highest_version(responses)
    assert latest == v2_newer

    stale = find_stale_replicas(responses, latest)
    assert stale == frozenset({1, 3})

    next_val = compute_next_version(
        responses=responses,
        writer_id=3,
        new_value="c",
        lamport_ts=20,
        now_wall_ns=1000,
    )
    assert next_val.version == 3
    assert next_val.writer_id == 3
    assert next_val.value == "c"


def test_heartbeat_detector_transitions() -> None:
    det = HeartbeatFailureDetector(
        node_id=1,
        peer_ids=[1, 2, 3],
        timeout_ms=200.0,
        initial_time_ns=0,
    )
    assert det.alive_peers() == frozenset({2, 3})

    # Em t = 150ms: nó 2 envia heartbeat, nó 3 fica silencioso
    assert det.record_heartbeat(peer_id=2, sequence=1, now_ns=150_000_000) is None
    assert det.evaluate_timeouts(now_ns=150_000_000) == []

    # Em t = 250ms: nó 3 ultrapassou 200ms (250ms > 200ms), mas nó 2 está a 100ms
    events = det.evaluate_timeouts(now_ns=250_000_000)
    assert len(events) == 1
    assert events[0].peer_id == 3
    assert events[0].new_status == PeerHealthStatus.SUSPECTED
    assert det.suspected_peers() == frozenset({3})

    # Em t = 300ms: nó 3 volta a enviar heartbeat -> recupera para ALIVE
    rec_ev = det.record_heartbeat(peer_id=3, sequence=2, now_ns=300_000_000)
    assert rec_ev is not None
    assert rec_ev.new_status == PeerHealthStatus.ALIVE
    assert det.alive_peers() == frozenset({2, 3})


def test_fault_injector_reproducibility_with_seed() -> None:
    inj1 = FaultInjector(seed=12345)
    inj2 = FaultInjector(seed=12345)
    inj1.configure(drop_probability=0.35, delay_ms=10.0, jitter_ms=5.0)
    inj2.configure(drop_probability=0.35, delay_ms=10.0, jitter_ms=5.0)

    decisions1 = [inj1.intercept(peer_id=2) for _ in range(30)]
    decisions2 = [inj2.intercept(peer_id=2) for _ in range(30)]
    assert decisions1 == decisions2

    inj1.configure(partitioned_peers=[4, 5])
    part_dec = inj1.intercept(peer_id=4)
    assert not part_dec.allowed and part_dec.reason == "network_partition"

    inj1.configure(crashed=True)
    crash_dec = inj1.intercept(peer_id=2)
    assert not crash_dec.allowed and crash_dec.reason == "node_crashed"


def test_latency_percentiles_and_quorum_verifier() -> None:
    samples = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    summary = summarize_latencies(samples)
    assert summary.count == 10
    assert summary.mean_ms == pytest.approx(55.0)
    assert summary.p50_ms == pytest.approx(55.0)
    assert summary.p95_ms > summary.p50_ms
    assert summary.p99_ms >= summary.p95_ms

    ops_ok = [
        QuorumOperationRecord("WRITE", 1, "k", "v1", 1, 1, 100, 200, True),
        QuorumOperationRecord("READ", 2, "k", "v1", 1, 1, 250, 300, True),
    ]
    assert verify_quorum_consistency(ops_ok).passed

    ops_stale = [
        QuorumOperationRecord("WRITE", 1, "k", "v2", 2, 1, 100, 200, True),
        QuorumOperationRecord("READ", 3, "k", "v1", 1, 1, 250, 300, True),
    ]
    assert not verify_quorum_consistency(ops_stale).passed

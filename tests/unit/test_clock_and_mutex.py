"""Testes unitários de Relógio de Lamport, Ricart-Agrawala e Verificador de Safety."""

from __future__ import annotations

import pytest

from mc714_p2.domain.clock import LamportClock, LogicalTimestamp
from mc714_p2.domain.metrics import verify_mutual_exclusion_safety
from mc714_p2.domain.mutex import (
    CriticalSectionRecord,
    MutexState,
    RicartAgrawalaMutex,
)


def test_lamport_clock_progression_and_total_order() -> None:
    c1 = LamportClock(node_id=1)
    c2 = LamportClock(node_id=2)

    assert c1.time == 0
    assert c1.tick() == 1
    assert c1.tick() == 2

    # Recepção de mensagem com timestamp 2 avança c2 para max(0, 2) + 1 = 3
    assert c2.update(2) == 3
    assert c1.update(3) == 4

    # Desempate determinístico por node_id quando timestamps são iguais
    t_node1 = LogicalTimestamp(timestamp=5, node_id=1)
    t_node2 = LogicalTimestamp(timestamp=5, node_id=2)
    assert t_node1 < t_node2
    assert c1.snapshot() == LogicalTimestamp(timestamp=4, node_id=1)


def test_lamport_clock_invalid_inputs() -> None:
    with pytest.raises(ValueError):
        LamportClock(node_id=-1)
    with pytest.raises(ValueError):
        LamportClock(node_id=1, initial_time=-5)
    clock = LamportClock(node_id=1)
    with pytest.raises(ValueError):
        clock.update(-1)


def test_ricart_agrawala_contention_and_tie_breaking() -> None:
    c1 = LamportClock(node_id=1)
    c2 = LamportClock(node_id=2)
    m1 = RicartAgrawalaMutex(node_id=1, clock=c1, peer_ids=[1, 2])
    m2 = RicartAgrawalaMutex(node_id=2, clock=c2, peer_ids=[1, 2])

    # Ambos pedem SC concorrentemente com timestamp lógico = 1
    req1_ts, peers1 = m1.prepare_request(now_wall_ns=1_000)
    req2_ts, peers2 = m2.prepare_request(now_wall_ns=1_000)
    assert req1_ts == 1 and req2_ts == 1
    assert peers1 == frozenset({2})
    assert peers2 == frozenset({1})

    # Nó 1 tem prioridade porque (1, 1) < (1, 2):
    # - Nó 1 DEFERE pedido do Nó 2
    grant_to_2, _ = m1.handle_request(requester_id=2, requester_ts=req2_ts)
    assert not grant_to_2
    assert 2 in m1.deferred_replies

    # - Nó 2 CONCEDE imediatamente ao Nó 1
    grant_to_1, reply2_ts = m2.handle_request(requester_id=1, requester_ts=req1_ts)
    assert grant_to_1

    # Nó 1 recebe o REPLY do Nó 2 e entra em HELD
    entered1 = m1.handle_reply(
        responder_id=2, responder_ts=reply2_ts, now_wall_ns=2_000
    )
    assert entered1
    assert m1.state == MutexState.HELD
    assert m2.state == MutexState.WANTED

    # Nó 1 libera a SC e notifica o Nó 2 (que estava em deferred_replies)
    exit1_ts, deferred = m1.release(now_wall_ns=3_000)
    assert m1.state == MutexState.RELEASED
    assert deferred == frozenset({2})

    # Nó 2 recebe o REPLY postergado do Nó 1 e entra em HELD
    entered2 = m2.handle_reply(responder_id=1, responder_ts=exit1_ts, now_wall_ns=4_000)
    assert entered2
    assert m2.state == MutexState.HELD
    m2.release(now_wall_ns=5_000)

    # Verifica Safety sobre o histórico combinado dos dois nós
    combined_history = list(m1.history) + list(m2.history)
    safety = verify_mutual_exclusion_safety(combined_history)
    assert safety.passed
    assert safety.total_events == 2
    assert len(safety.violations) == 0


def test_ricart_agrawala_unblocks_when_peer_suspected() -> None:
    c1 = LamportClock(node_id=1)
    m1 = RicartAgrawalaMutex(node_id=1, clock=c1, peer_ids=[2, 3])

    _, peers = m1.prepare_request(now_wall_ns=100)
    assert peers == frozenset({2, 3})

    # Nó 2 responde normalmente, mas Nó 3 sofre crash-stop
    assert not m1.handle_reply(responder_id=2, responder_ts=2, now_wall_ns=200)
    assert m1.state == MutexState.WANTED

    # Detector de falhas marca Nó 3 como SUSPECTED -> Nó 1 entra na SC!
    unblocked = m1.on_peer_suspected(peer_id=3, now_wall_ns=300)
    assert unblocked
    assert m1.state == MutexState.HELD
    assert 3 not in m1.active_peers

    m1.release(now_wall_ns=400)
    m1.on_peer_recovered(peer_id=3)
    assert 3 in m1.active_peers


def test_safety_verifier_detects_overlapping_critical_sections() -> None:
    bad_records = [
        CriticalSectionRecord(
            node_id=1,
            request_lamport=1,
            enter_lamport=3,
            exit_lamport=6,
            enter_wall_ns=1000,
            exit_wall_ns=3000,
        ),
        CriticalSectionRecord(
            node_id=2,
            request_lamport=2,
            enter_lamport=5,  # Violação: entrou antes de exit_lamport=6
            exit_lamport=8,
            enter_wall_ns=2000,  # Violação: sobrepôs [1000, 3000]
            exit_wall_ns=4000,
        ),
    ]
    res = verify_mutual_exclusion_safety(bad_records)
    assert not res.passed
    assert len(res.violations) == 2

"""Detector de Falhas Distribuído baseado em Heartbeat (Algoritmo 3 / Bônus)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class PeerHealthStatus(StrEnum):
    """Estado de saúde observado de um peer remoto."""

    ALIVE = "ALIVE"
    SUSPECTED = "SUSPECTED"


@dataclass(frozen=True, slots=True)
class HeartbeatTransitionEvent:
    """Evento de transição de estado de um peer (ALIVE <-> SUSPECTED)."""

    observer_id: int
    peer_id: int
    old_status: PeerHealthStatus
    new_status: PeerHealthStatus
    timestamp_ns: int
    elapsed_ms: float
    sequence: int


@dataclass(slots=True)
class PeerHeartbeatState:
    """Estado interno monitorado para um único peer."""

    peer_id: int
    status: PeerHealthStatus
    last_seen_ns: int
    last_sequence: int = 0
    suspect_count: int = 0


class HeartbeatFailureDetector:
    """Detector de falhas baseado em heartbeats periódicos e timeout explícito."""

    def __init__(
        self,
        node_id: int,
        peer_ids: Iterable[int],
        timeout_ms: float,
        initial_time_ns: int = 0,
    ) -> None:
        if timeout_ms <= 0:
            raise ValueError("timeout_ms deve ser positivo.")
        self._node_id = node_id
        self._timeout_ns = int(timeout_ms * 1_000_000)
        self._seq_counter = 0
        self._peers: dict[int, PeerHeartbeatState] = {
            pid: PeerHeartbeatState(
                peer_id=pid,
                status=PeerHealthStatus.ALIVE,
                last_seen_ns=initial_time_ns,
            )
            for pid in peer_ids
            if pid != node_id
        }
        self._events: list[HeartbeatTransitionEvent] = []

    @property
    def node_id(self) -> int:
        return self._node_id

    @property
    def timeout_ms(self) -> float:
        return self._timeout_ns / 1_000_000.0

    @property
    def events(self) -> tuple[HeartbeatTransitionEvent, ...]:
        return tuple(self._events)

    def next_sequence(self) -> int:
        """Gera o próximo número de sequência para envio de heartbeat."""
        self._seq_counter += 1
        return self._seq_counter

    def set_timeout_ms(self, timeout_ms: float) -> None:
        """Atualiza dinamicamente o timeout de suspeita (para experimentos)."""
        if timeout_ms <= 0:
            raise ValueError("timeout_ms deve ser positivo.")
        self._timeout_ns = int(timeout_ms * 1_000_000)

    def record_heartbeat(
        self,
        peer_id: int,
        sequence: int,
        now_ns: int,
    ) -> HeartbeatTransitionEvent | None:
        """Registra recebimento de heartbeat (ou mensagem viva) de peer_id.

        Se o peer estava SUSPECTED, transita para ALIVE e emite evento de recuperação.
        """
        state = self._peers.get(peer_id)
        if state is None:
            return None

        elapsed_ms = max(0.0, (now_ns - state.last_seen_ns) / 1_000_000.0)
        state.last_seen_ns = max(state.last_seen_ns, now_ns)
        state.last_sequence = max(state.last_sequence, sequence)

        if state.status == PeerHealthStatus.SUSPECTED:
            event = HeartbeatTransitionEvent(
                observer_id=self._node_id,
                peer_id=peer_id,
                old_status=PeerHealthStatus.SUSPECTED,
                new_status=PeerHealthStatus.ALIVE,
                timestamp_ns=now_ns,
                elapsed_ms=elapsed_ms,
                sequence=state.last_sequence,
            )
            state.status = PeerHealthStatus.ALIVE
            self._events.append(event)
            return event
        return None

    def evaluate_timeouts(self, now_ns: int) -> list[HeartbeatTransitionEvent]:
        """Verifica quais peers ALIVE excederam o timeout sem enviar heartbeat."""
        transitions: list[HeartbeatTransitionEvent] = []
        for state in self._peers.values():
            delta_ns = now_ns - state.last_seen_ns
            if state.status == PeerHealthStatus.ALIVE and delta_ns > self._timeout_ns:
                elapsed_ms = delta_ns / 1_000_000.0
                state.status = PeerHealthStatus.SUSPECTED
                state.suspect_count += 1
                event = HeartbeatTransitionEvent(
                    observer_id=self._node_id,
                    peer_id=state.peer_id,
                    old_status=PeerHealthStatus.ALIVE,
                    new_status=PeerHealthStatus.SUSPECTED,
                    timestamp_ns=now_ns,
                    elapsed_ms=elapsed_ms,
                    sequence=state.last_sequence,
                )
                self._events.append(event)
                transitions.append(event)
        return transitions

    def alive_peers(self) -> frozenset[int]:
        """Retorna o conjunto de peers considerados ALIVE no momento."""
        return frozenset(
            pid
            for pid, st in self._peers.items()
            if st.status == PeerHealthStatus.ALIVE
        )

    def suspected_peers(self) -> frozenset[int]:
        """Retorna o conjunto de peers considerados SUSPECTED no momento."""
        return frozenset(
            pid
            for pid, st in self._peers.items()
            if st.status == PeerHealthStatus.SUSPECTED
        )

    def peer_states(self) -> dict[int, PeerHeartbeatState]:
        """Retorna snapshot dos estados de cada peer monitorado."""
        return {
            pid: PeerHeartbeatState(
                peer_id=st.peer_id,
                status=st.status,
                last_seen_ns=st.last_seen_ns,
                last_sequence=st.last_sequence,
                suspect_count=st.suspect_count,
            )
            for pid, st in self._peers.items()
        }

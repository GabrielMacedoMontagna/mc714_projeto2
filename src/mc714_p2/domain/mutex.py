"""Algoritmo de Exclusão Mútua Distribuída de Ricart-Agrawala integrado a Lamport."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from mc714_p2.domain.clock import LamportClock, LogicalTimestamp


class MutexState(StrEnum):
    """Estados do protocolo de Ricart-Agrawala."""

    RELEASED = "RELEASED"
    WANTED = "WANTED"
    HELD = "HELD"


@dataclass(frozen=True, slots=True)
class CriticalSectionRecord:
    """Registro auditável de entrada e saída na Seção Crítica (SC)."""

    node_id: int
    request_lamport: int
    enter_lamport: int
    exit_lamport: int
    enter_wall_ns: int
    exit_wall_ns: int
    resource_tag: str = "default"


class RicartAgrawalaMutex:
    """Máquina de estados pura de Ricart-Agrawala com suporte a visão de falhas."""

    def __init__(
        self,
        node_id: int,
        clock: LamportClock,
        peer_ids: Iterable[int],
    ) -> None:
        self._node_id = node_id
        self._clock = clock
        self._all_peers: set[int] = {pid for pid in peer_ids if pid != node_id}
        self._active_peers: set[int] = set(self._all_peers)
        self._state: MutexState = MutexState.RELEASED
        self._request_timestamp: int | None = None
        self._enter_timestamp: int | None = None
        self._enter_wall_ns: int | None = None
        self._pending_replies: set[int] = set()
        self._deferred_replies: set[int] = set()
        self._history: list[CriticalSectionRecord] = []

    @property
    def node_id(self) -> int:
        return self._node_id

    @property
    def state(self) -> MutexState:
        return self._state

    @property
    def request_timestamp(self) -> int | None:
        return self._request_timestamp

    @property
    def pending_replies(self) -> frozenset[int]:
        return frozenset(self._pending_replies)

    @property
    def deferred_replies(self) -> frozenset[int]:
        return frozenset(self._deferred_replies)

    @property
    def active_peers(self) -> frozenset[int]:
        return frozenset(self._active_peers)

    @property
    def history(self) -> tuple[CriticalSectionRecord, ...]:
        return tuple(self._history)

    def prepare_request(self, now_wall_ns: int) -> tuple[int, frozenset[int]]:
        """Inicia pedido de entrada na Seção Crítica.

        Retorna (request_lamport_ts, conjunto_de_peers_ativos_a_consultar).
        Se não houver peers ativos, transita diretamente para HELD.
        """
        if self._state != MutexState.RELEASED:
            raise RuntimeError(
                f"Nó {self._node_id} não pode pedir SC no estado {self._state}."
            )
        req_ts = self._clock.tick()
        self._request_timestamp = req_ts
        self._pending_replies = set(self._active_peers)
        if not self._pending_replies:
            self._enter_critical_section(now_wall_ns)
            return req_ts, frozenset()
        self._state = MutexState.WANTED
        return req_ts, frozenset(self._pending_replies)

    def handle_request(
        self,
        requester_id: int,
        requester_ts: int,
    ) -> tuple[bool, int]:
        """Processa REQUEST(requester_ts, requester_id) de outro nó.

        Retorna (grant_immediately, reply_lamport_ts).
        Se grant_immediately for False, o pedido foi postergado em deferred_replies.
        """
        self._clock.update(requester_ts)
        if requester_id in self._all_peers:
            self._active_peers.add(requester_id)

        if self._should_defer(requester_id, requester_ts):
            self._deferred_replies.add(requester_id)
            return False, self._clock.time

        reply_ts = self._clock.tick()
        return True, reply_ts

    def handle_reply(
        self,
        responder_id: int,
        responder_ts: int,
        now_wall_ns: int,
    ) -> bool:
        """Processa REPLY de um peer. Retorna True se o nó acabou de entrar em HELD."""
        self._clock.update(responder_ts)
        if self._state != MutexState.WANTED:
            return False
        self._pending_replies.discard(responder_id)
        if not self._pending_replies:
            self._enter_critical_section(now_wall_ns)
            return True
        return False

    def release(
        self,
        now_wall_ns: int,
        resource_tag: str = "default",
    ) -> tuple[int, frozenset[int]]:
        """Sai da Seção Crítica e retorna (exit_lamport_ts, deferred_peers)."""
        if self._state != MutexState.HELD:
            raise RuntimeError(
                f"Nó {self._node_id} não está em HELD (estado={self._state})."
            )
        exit_ts = self._clock.tick()
        req_ts = (
            self._request_timestamp if self._request_timestamp is not None else exit_ts
        )
        enter_ts = (
            self._enter_timestamp if self._enter_timestamp is not None else req_ts
        )
        enter_ns = (
            self._enter_wall_ns if self._enter_wall_ns is not None else now_wall_ns
        )

        self._history.append(
            CriticalSectionRecord(
                node_id=self._node_id,
                request_lamport=req_ts,
                enter_lamport=enter_ts,
                exit_lamport=exit_ts,
                enter_wall_ns=enter_ns,
                exit_wall_ns=max(now_wall_ns, enter_ns),
                resource_tag=resource_tag,
            )
        )
        self._state = MutexState.RELEASED
        self._request_timestamp = None
        self._enter_timestamp = None
        self._enter_wall_ns = None

        to_notify = frozenset(self._deferred_replies & self._active_peers)
        self._deferred_replies.clear()
        return exit_ts, to_notify

    def on_peer_suspected(self, peer_id: int, now_wall_ns: int) -> bool:
        """Remove peer suspeito/falho da visão ativa.

        Retorna True se essa remoção destravou a entrada do nó atual em HELD.
        """
        self._active_peers.discard(peer_id)
        self._deferred_replies.discard(peer_id)
        if self._state == MutexState.WANTED and peer_id in self._pending_replies:
            self._pending_replies.discard(peer_id)
            if not self._pending_replies:
                self._enter_critical_section(now_wall_ns)
                return True
        return False

    def on_peer_recovered(self, peer_id: int) -> None:
        """Reintegra um peer recuperado ao conjunto de nós ativos."""
        if peer_id in self._all_peers:
            self._active_peers.add(peer_id)

    def abort_wanted(self) -> None:
        """Cancela pedido em estado WANTED (ex.: timeout explícito da aplicação)."""
        if self._state == MutexState.WANTED:
            self._state = MutexState.RELEASED
            self._request_timestamp = None
            self._pending_replies.clear()

    def _should_defer(self, requester_id: int, requester_ts: int) -> bool:
        if self._state == MutexState.HELD:
            return True
        if self._state == MutexState.WANTED and self._request_timestamp is not None:
            my_order = LogicalTimestamp(self._request_timestamp, self._node_id)
            their_order = LogicalTimestamp(requester_ts, requester_id)
            return my_order < their_order
        return False

    def _enter_critical_section(self, now_wall_ns: int) -> None:
        self._state = MutexState.HELD
        self._enter_timestamp = self._clock.tick()
        self._enter_wall_ns = now_wall_ns

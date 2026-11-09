"""Coordenador assíncrono do nó distribuído (integra Domínio, Relógio e RPC)."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass

import grpc

from mc714_p2.domain.clock import LamportClock
from mc714_p2.domain.fault_injector import FaultInjector
from mc714_p2.domain.heartbeat import HeartbeatFailureDetector, PeerHealthStatus
from mc714_p2.domain.mutex import MutexState, RicartAgrawalaMutex
from mc714_p2.domain.quorum import (
    EMPTY_VALUE,
    LocalReplicaStore,
    QuorumReadResult,
    QuorumWriteResult,
    VersionedValue,
    compute_next_version,
    find_stale_replicas,
    select_highest_version,
)
from mc714_p2.node.config import NodeSettings
from mc714_p2.rpc.client import PeerRpcClient


@dataclass(slots=True)
class ProtocolCounters:
    """Contadores auditáveis de mensagens de protocolo trocadas na rede."""

    mutex_sent: int = 0
    mutex_received: int = 0
    quorum_sent: int = 0
    quorum_received: int = 0
    heartbeat_sent: int = 0
    heartbeat_received: int = 0

    @property
    def total_mutex(self) -> int:
        return self.mutex_sent + self.mutex_received


@dataclass(frozen=True, slots=True)
class CriticalSectionOutcome:
    """Resultado de uma execução completa de Seção Crítica solicitada por cliente."""

    acquired: bool
    node_id: int
    request_lamport_ts: int
    enter_lamport_ts: int
    exit_lamport_ts: int
    enter_wall_ns: int
    exit_wall_ns: int
    wait_latency_ms: float
    protocol_messages_used: int
    error_message: str = ""


class DistributedNode:
    """Orquestrador de um nó lógico do sistema distribuído."""

    def __init__(
        self,
        settings: NodeSettings,
        rpc_client: PeerRpcClient | None = None,
    ) -> None:
        self.settings = settings
        self.node_id = settings.node_id
        self.quorum_cfg = settings.quorum_config()

        remote_eps = settings.remote_peer_endpoints()
        self.rpc_client = (
            rpc_client if rpc_client is not None else PeerRpcClient(remote_eps)
        )

        now_ns = time.monotonic_ns()
        self.clock = LamportClock(node_id=self.node_id)
        self.mutex = RicartAgrawalaMutex(
            node_id=self.node_id,
            clock=self.clock,
            peer_ids=remote_eps.keys(),
        )
        self.replica_store = LocalReplicaStore()
        self.failure_detector = HeartbeatFailureDetector(
            node_id=self.node_id,
            peer_ids=remote_eps.keys(),
            timeout_ms=settings.hb_timeout_ms,
            initial_time_ns=now_ns,
        )
        self.fault_injector = FaultInjector(seed=settings.fault_seed)
        self.counters = ProtocolCounters()

        self._state_lock = asyncio.Lock()
        self._cs_client_lock = asyncio.Lock()
        self._held_event = asyncio.Event()
        self._hb_task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def rpc_timeout_sec(self) -> float:
        return max(0.05, self.settings.rpc_timeout_ms / 1000.0)

    async def start_background_tasks(self) -> None:
        """Inicia o loop assíncrono do Detector de Falhas por Heartbeat."""
        self._stopping = False
        if self._hb_task is None or self._hb_task.done():
            self._hb_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Encerra tarefas em background e fecha canais gRPC."""
        self._stopping = True
        if self._hb_task is not None:
            self._hb_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._hb_task
            self._hb_task = None
        await self.rpc_client.close()

    async def check_inbound_fault(
        self,
        sender_id: int | None,
        context: grpc.aio.ServicerContext,
    ) -> bool:
        """Aplica política de injeção de falhas na chegada de um RPC."""
        decision = self.fault_injector.intercept(sender_id)
        if not decision.allowed:
            await context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"Falha simulada ({decision.reason}) no nó {self.node_id}",
            )
            return False
        if decision.delay_seconds > 0.0:
            await asyncio.sleep(decision.delay_seconds)
        return True

    async def check_outbound_fault(self, peer_id: int) -> bool:
        """Aplica política de injeção de falhas antes do envio de um RPC."""
        decision = self.fault_injector.intercept(peer_id)
        if not decision.allowed:
            return False
        if decision.delay_seconds > 0.0:
            await asyncio.sleep(decision.delay_seconds)
        return True

    # -------------------------------------------------------------------------
    # 1. Detector de Falhas (Heartbeat) + Integração com Ricart-Agrawala
    # -------------------------------------------------------------------------

    async def handle_heartbeat(
        self,
        sender_id: int,
        sequence: int,
        sender_lamport_ts: int,
    ) -> tuple[int, bool]:
        """Processa recebimento de Heartbeat de sender_id."""
        async with self._state_lock:
            self.counters.heartbeat_received += 1
            reply_ts = self.clock.update(sender_lamport_ts)
            now_ns = time.monotonic_ns()
            event = self.failure_detector.record_heartbeat(
                peer_id=sender_id,
                sequence=sequence,
                now_ns=now_ns,
            )
            if event is not None and event.new_status == PeerHealthStatus.ALIVE:
                self.mutex.on_peer_recovered(sender_id)
            return reply_ts, True

    async def run_heartbeat_step(self) -> None:
        """Executa uma rodada de envio de heartbeats e avaliação de timeouts."""
        if self.fault_injector.is_crashed:
            return

        async with self._state_lock:
            seq = self.failure_detector.next_sequence()
            lamport_ts = self.clock.tick()
            peer_ids = list(self.rpc_client.peer_ids)

        if peer_ids:
            await asyncio.gather(
                *(
                    self._send_single_heartbeat(pid, seq, lamport_ts)
                    for pid in peer_ids
                ),
                return_exceptions=True,
            )

        async with self._state_lock:
            now_ns = time.monotonic_ns()
            wall_ns = time.time_ns()
            transitions = self.failure_detector.evaluate_timeouts(now_ns)
            for ev in transitions:
                if ev.new_status == PeerHealthStatus.SUSPECTED:
                    entered = self.mutex.on_peer_suspected(ev.peer_id, wall_ns)
                    if entered:
                        self._held_event.set()

    async def _send_single_heartbeat(
        self,
        peer_id: int,
        seq: int,
        lamport_ts: int,
    ) -> None:
        if not await self.check_outbound_fault(peer_id):
            return
        self.counters.heartbeat_sent += 1
        try:
            ack = await self.rpc_client.send_heartbeat(
                peer_id=peer_id,
                sender_id=self.node_id,
                sequence=seq,
                lamport_ts=lamport_ts,
                timeout_sec=self.rpc_timeout_sec,
            )
        except Exception:
            return

        async with self._state_lock:
            self.counters.heartbeat_received += 1
            self.clock.update(int(ack.lamport_ts))
            if peer_id in self.failure_detector.suspected_peers():
                event = self.failure_detector.record_heartbeat(
                    peer_id=peer_id,
                    sequence=int(ack.sequence),
                    now_ns=time.monotonic_ns(),
                )
                if event is not None and event.new_status == PeerHealthStatus.ALIVE:
                    self.mutex.on_peer_recovered(peer_id)

    async def _heartbeat_loop(self) -> None:
        interval_sec = max(0.01, self.settings.hb_interval_ms / 1000.0)
        while not self._stopping:
            await self.run_heartbeat_step()
            await asyncio.sleep(interval_sec)

    # -------------------------------------------------------------------------
    # 2. Exclusão Mútua Distribuída (Ricart-Agrawala + Relógio de Lamport)
    # -------------------------------------------------------------------------

    async def handle_mutex_request(
        self,
        requester_id: int,
        request_lamport_ts: int,
    ) -> tuple[bool, int]:
        """Trata pedido REQUEST(ts, id) de entrada na Seção Crítica."""
        async with self._state_lock:
            self.counters.mutex_received += 1
            now_ns = time.monotonic_ns()
            self.failure_detector.record_heartbeat(requester_id, 0, now_ns)
            granted_now, reply_ts = self.mutex.handle_request(
                requester_id=requester_id,
                requester_ts=request_lamport_ts,
            )
            if granted_now:
                self.counters.mutex_sent += 1
            return granted_now, reply_ts

    async def handle_deferred_reply(
        self,
        responder_id: int,
        responder_lamport_ts: int,
    ) -> bool:
        """Recebe REPLY postergado de um peer que acabou de sair da SC."""
        async with self._state_lock:
            self.counters.mutex_received += 1
            entered = self.mutex.handle_reply(
                responder_id=responder_id,
                responder_ts=responder_lamport_ts,
                now_wall_ns=time.time_ns(),
            )
            if entered:
                self._held_event.set()
            return True

    async def execute_critical_section(
        self,
        resource_tag: str = "default",
        hold_ms: float = 10.0,
        timeout_ms: float = 3000.0,
        optional_quorum_key: str = "",
        optional_quorum_value: str = "",
    ) -> CriticalSectionOutcome:
        """Adquire a Seção Crítica via Ricart-Agrawala, executa e libera."""
        if self.fault_injector.is_crashed:
            return self._failed_cs_outcome("Nó em estado CRASHED.")

        async with self._cs_client_lock:
            start_mono = time.perf_counter()
            msg_before = self.counters.total_mutex

            async with self._state_lock:
                self._held_event.clear()
                req_ts, peers_to_ask = self.mutex.prepare_request(time.time_ns())
                if self.mutex.state == MutexState.HELD:
                    self._held_event.set()

            acquired = await self._wait_for_cs_entry(req_ts, peers_to_ask, timeout_ms)
            if not acquired:
                async with self._state_lock:
                    self.mutex.abort_wanted()
                return self._failed_cs_outcome("Timeout aguardando Seção Crítica.")

            wait_latency_ms = (time.perf_counter() - start_mono) * 1000.0
            await self._run_inside_critical_section(
                hold_ms,
                optional_quorum_key,
                optional_quorum_value,
            )

            async with self._state_lock:
                exit_ts, deferred_peers = self.mutex.release(
                    now_wall_ns=time.time_ns(),
                    resource_tag=resource_tag,
                )
                last_rec = self.mutex.history[-1]

            if deferred_peers:
                await asyncio.gather(
                    *(
                        self._send_deferred_reply(pid, exit_ts)
                        for pid in deferred_peers
                    ),
                    return_exceptions=True,
                )

            msg_used = max(0, self.counters.total_mutex - msg_before)
            return CriticalSectionOutcome(
                acquired=True,
                node_id=self.node_id,
                request_lamport_ts=last_rec.request_lamport,
                enter_lamport_ts=last_rec.enter_lamport,
                exit_lamport_ts=last_rec.exit_lamport,
                enter_wall_ns=last_rec.enter_wall_ns,
                exit_wall_ns=last_rec.exit_wall_ns,
                wait_latency_ms=wait_latency_ms,
                protocol_messages_used=msg_used,
            )

    async def _wait_for_cs_entry(
        self,
        req_ts: int,
        initial_peers: frozenset[int],
        timeout_ms: float,
    ) -> bool:
        if self._held_event.is_set():
            return True

        await self._broadcast_mutex_requests(req_ts, initial_peers)
        deadline = time.perf_counter() + max(0.05, timeout_ms / 1000.0)
        retry_interval = max(0.04, self.rpc_timeout_sec * 0.5)

        while time.perf_counter() < deadline:
            remaining = deadline - time.perf_counter()
            wait_slice = min(retry_interval, max(0.005, remaining))
            try:
                await asyncio.wait_for(self._held_event.wait(), timeout=wait_slice)
                return True
            except TimeoutError:
                await self.run_heartbeat_step()
                async with self._state_lock:
                    if self.mutex.state == MutexState.HELD:
                        return True
                    pending = self.mutex.pending_replies
                if pending:
                    await self._broadcast_mutex_requests(req_ts, pending)
        return self._held_event.is_set()

    async def _broadcast_mutex_requests(
        self,
        req_ts: int,
        peers: frozenset[int],
    ) -> None:
        if not peers:
            return
        await asyncio.gather(
            *(self._send_mutex_request(pid, req_ts) for pid in peers),
            return_exceptions=True,
        )

    async def _send_mutex_request(self, peer_id: int, req_ts: int) -> None:
        if not await self.check_outbound_fault(peer_id):
            return
        self.counters.mutex_sent += 1
        try:
            ack = await self.rpc_client.request_access(
                peer_id=peer_id,
                requester_id=self.node_id,
                request_lamport_ts=req_ts,
                resource_tag="default",
                timeout_sec=self.rpc_timeout_sec,
            )
        except Exception:
            return

        async with self._state_lock:
            if ack.granted_immediately:
                self.counters.mutex_received += 1
                entered = self.mutex.handle_reply(
                    responder_id=peer_id,
                    responder_ts=int(ack.responder_lamport_ts),
                    now_wall_ns=time.time_ns(),
                )
                if entered:
                    self._held_event.set()
            else:
                self.clock.update(int(ack.responder_lamport_ts))

    async def _send_deferred_reply(self, peer_id: int, exit_ts: int) -> None:
        for _attempt in range(2):
            if not await self.check_outbound_fault(peer_id):
                return
            self.counters.mutex_sent += 1
            try:
                await self.rpc_client.deliver_deferred_reply(
                    peer_id=peer_id,
                    responder_id=self.node_id,
                    responder_lamport_ts=exit_ts,
                    timeout_sec=self.rpc_timeout_sec,
                )
                return
            except Exception:
                await asyncio.sleep(0.01)

    async def _run_inside_critical_section(
        self,
        hold_ms: float,
        quorum_key: str,
        quorum_value: str,
    ) -> None:
        if hold_ms > 0.0:
            await asyncio.sleep(hold_ms / 1000.0)
        if quorum_key:
            await self.quorum_write(key=quorum_key, value=quorum_value)

    def _failed_cs_outcome(self, reason: str) -> CriticalSectionOutcome:
        return CriticalSectionOutcome(
            acquired=False,
            node_id=self.node_id,
            request_lamport_ts=0,
            enter_lamport_ts=0,
            exit_lamport_ts=0,
            enter_wall_ns=0,
            exit_wall_ns=0,
            wait_latency_ms=0.0,
            protocol_messages_used=0,
            error_message=reason,
        )

    # -------------------------------------------------------------------------
    # 3. Replicação Baseada em Quorum (Gifford + Read-Repair)
    # -------------------------------------------------------------------------

    async def handle_replica_read(
        self,
        key: str,
        sender_lamport_ts: int,
    ) -> tuple[VersionedValue, int]:
        """Lê valor versionado da réplica local."""
        async with self._state_lock:
            self.counters.quorum_received += 1
            resp_ts = self.clock.update(sender_lamport_ts)
            val = self.replica_store.get(key)
            return val, resp_ts

    async def handle_replica_write(
        self,
        key: str,
        candidate: VersionedValue,
        sender_lamport_ts: int,
    ) -> tuple[bool, VersionedValue, int]:
        """Aplica escrita versionada monotônica na réplica local."""
        async with self._state_lock:
            self.counters.quorum_received += 1
            resp_ts = self.clock.update(sender_lamport_ts)
            applied, current = self.replica_store.put_if_newer(key, candidate)
            return applied, current, resp_ts

    async def quorum_read(
        self,
        key: str,
        enable_read_repair: bool = True,
        override_read_quorum: int = 0,
    ) -> QuorumReadResult:
        """Executa leitura distribuída com quórum N_R e Read-Repair opcional."""
        if self.fault_injector.is_crashed:
            raise RuntimeError(f"Nó coordenador {self.node_id} está CRASHED.")

        required_r = (
            override_read_quorum
            if override_read_quorum > 0
            else self.quorum_cfg.read_quorum
        )
        start_perf = time.perf_counter()
        responses = await self._gather_replica_reads(
            key=key,
            required_quorum=required_r,
            include_suspected_history=enable_read_repair,
        )

        if len(responses) < required_r:
            raise RuntimeError(
                f"Quórum de leitura não atingido: {len(responses)}/{required_r} ACKs."
            )

        latest = select_highest_version(responses)
        stale_nodes = find_stale_replicas(responses, latest)
        repaired: set[int] = set()

        if enable_read_repair and latest.found:
            if stale_nodes and len(responses) <= len(self.rpc_client.peer_ids):
                responses = await self._expand_reads_to_all_peers(key, responses)
                latest = select_highest_version(responses)
                stale_nodes = find_stale_replicas(responses, latest)
            if stale_nodes:
                repaired = await self._perform_read_repair(key, latest, stale_nodes)

        latency_ms = (time.perf_counter() - start_perf) * 1000.0
        return QuorumReadResult(
            key=key,
            value=latest,
            acks_received=len(responses),
            quorum_required=required_r,
            stale_nodes_repaired=frozenset(repaired),
            latency_ms=latency_ms,
        )

    async def quorum_write(
        self,
        key: str,
        value: str,
        override_write_quorum: int = 0,
    ) -> QuorumWriteResult:
        """Executa escrita distribuída em 2 fases com quórum N_W."""
        if self.fault_injector.is_crashed:
            raise RuntimeError(f"Nó coordenador {self.node_id} está CRASHED.")

        required_w = (
            override_write_quorum
            if override_write_quorum > 0
            else self.quorum_cfg.write_quorum
        )
        start_perf = time.perf_counter()

        # Fase 1: Consulta versões em um quórum de escrita N_W
        read_responses = await self._gather_replica_reads(
            key=key,
            required_quorum=required_w,
            include_suspected_history=False,
        )
        if len(read_responses) < required_w:
            raise RuntimeError(
                f"Quórum de consulta pré-escrita insuficiente: "
                f"{len(read_responses)}/{required_w} ACKs."
            )

        async with self._state_lock:
            write_lamport = self.clock.tick()
            candidate = compute_next_version(
                responses=read_responses,
                writer_id=self.node_id,
                new_value=value,
                lamport_ts=write_lamport,
                now_wall_ns=time.time_ns(),
            )

        # Fase 2: Propaga a nova versão (v_max + 1, node_id) ao quórum N_W
        write_acks = await self._gather_replica_writes(
            key=key,
            candidate=candidate,
            required_quorum=required_w,
            is_read_repair=False,
        )
        if write_acks < required_w:
            raise RuntimeError(
                f"Quórum de escrita não atingido: {write_acks}/{required_w} ACKs."
            )

        latency_ms = (time.perf_counter() - start_perf) * 1000.0
        return QuorumWriteResult(
            key=key,
            value=candidate,
            read_phase_acks=len(read_responses),
            write_phase_acks=write_acks,
            quorum_required=required_w,
            latency_ms=latency_ms,
        )

    def _prioritized_remote_peers(
        self,
        needed_remotes: int,
        include_suspected_history: bool,
    ) -> tuple[list[int], list[int]]:
        """Divide peers remotos em conjunto primário do quórum e fallback."""
        all_peers = sorted(self.rpc_client.peer_ids)
        if not all_peers:
            return [], []

        total = len(all_peers)
        start_idx = self.node_id % total
        rotated = all_peers[start_idx:] + all_peers[:start_idx]

        suspected = self.failure_detector.suspected_peers()
        healthy = [pid for pid in rotated if pid not in suspected]
        unhealthy = [pid for pid in rotated if pid in suspected]
        ordered = healthy + unhealthy

        if include_suspected_history and self._has_fault_history():
            return ordered, []

        primary = ordered[: max(0, needed_remotes)]
        fallback = ordered[max(0, needed_remotes) :]
        return primary, fallback

    def _has_fault_history(self) -> bool:
        snapshots = self.failure_detector.peer_states()
        return any(st.suspect_count > 0 for st in snapshots.values())

    async def _gather_replica_reads(
        self,
        key: str,
        required_quorum: int = 0,
        include_suspected_history: bool = False,
    ) -> dict[int, VersionedValue]:
        async with self._state_lock:
            lamport_ts = self.clock.tick()
            local_val = self.replica_store.get(key)
            needed_remotes = (
                len(self.rpc_client.peer_ids)
                if required_quorum <= 0
                else max(0, required_quorum - 1)
            )
            primary, fallback = self._prioritized_remote_peers(
                needed_remotes=needed_remotes,
                include_suspected_history=include_suspected_history,
            )

        responses: dict[int, VersionedValue] = {self.node_id: local_val}
        await self._query_peer_batch(key, lamport_ts, primary, responses)

        if len(responses) < required_quorum and fallback:
            await self._query_peer_batch(key, lamport_ts, fallback, responses)
        return responses

    async def _expand_reads_to_all_peers(
        self,
        key: str,
        current_responses: dict[int, VersionedValue],
    ) -> dict[int, VersionedValue]:
        missing_peers = [
            pid for pid in self.rpc_client.peer_ids if pid not in current_responses
        ]
        if not missing_peers:
            return current_responses
        async with self._state_lock:
            lamport_ts = self.clock.tick()
        expanded = dict(current_responses)
        await self._query_peer_batch(key, lamport_ts, missing_peers, expanded)
        return expanded

    async def _query_peer_batch(
        self,
        key: str,
        lamport_ts: int,
        peers: list[int],
        out_responses: dict[int, VersionedValue],
    ) -> None:
        if not peers:
            return
        results = await asyncio.gather(
            *(self._query_single_replica(pid, key, lamport_ts) for pid in peers),
            return_exceptions=True,
        )
        for pid, res in zip(peers, results, strict=True):
            if isinstance(res, VersionedValue):
                out_responses[pid] = res

    async def _query_single_replica(
        self,
        peer_id: int,
        key: str,
        lamport_ts: int,
    ) -> VersionedValue | None:
        if not await self.check_outbound_fault(peer_id):
            return None
        self.counters.quorum_sent += 1
        try:
            ack = await self.rpc_client.replica_read(
                peer_id=peer_id,
                coordinator_id=self.node_id,
                key=key,
                lamport_ts=lamport_ts,
                timeout_sec=self.rpc_timeout_sec,
            )
        except Exception:
            return None

        async with self._state_lock:
            self.counters.quorum_received += 1
            self.clock.update(int(ack.responder_lamport_ts))

        if not ack.found or ack.version <= 0:
            return EMPTY_VALUE
        return VersionedValue(
            value=ack.value,
            version=int(ack.version),
            writer_id=int(ack.writer_id),
            lamport_ts=int(ack.value_lamport_ts),
        )

    async def _gather_replica_writes(
        self,
        key: str,
        candidate: VersionedValue,
        required_quorum: int,
        is_read_repair: bool,
    ) -> int:
        async with self._state_lock:
            sender_ts = self.clock.tick()
            self.replica_store.put_if_newer(key, candidate)
            needed_remotes = max(0, required_quorum - 1)
            primary, fallback = self._prioritized_remote_peers(
                needed_remotes=needed_remotes,
                include_suspected_history=False,
            )

        acks = 1 + await self._write_peer_batch(
            key, candidate, sender_ts, is_read_repair, primary
        )
        if acks < required_quorum and fallback:
            acks += await self._write_peer_batch(
                key, candidate, sender_ts, is_read_repair, fallback
            )
        return acks

    async def _write_peer_batch(
        self,
        key: str,
        candidate: VersionedValue,
        sender_ts: int,
        is_read_repair: bool,
        peers: list[int],
    ) -> int:
        if not peers:
            return 0
        results = await asyncio.gather(
            *(
                self._write_single_replica(
                    pid, key, candidate, sender_ts, is_read_repair
                )
                for pid in peers
            ),
            return_exceptions=True,
        )
        return sum(1 for res in results if res is True)

    async def _write_single_replica(
        self,
        peer_id: int,
        key: str,
        candidate: VersionedValue,
        sender_ts: int,
        is_read_repair: bool,
    ) -> bool:
        if not await self.check_outbound_fault(peer_id):
            return False
        self.counters.quorum_sent += 1
        try:
            ack = await self.rpc_client.replica_write(
                peer_id=peer_id,
                coordinator_id=self.node_id,
                key=key,
                value=candidate.value,
                version=candidate.version,
                writer_id=candidate.writer_id,
                value_lamport_ts=candidate.lamport_ts,
                sender_lamport_ts=sender_ts,
                is_read_repair=is_read_repair,
                timeout_sec=self.rpc_timeout_sec,
            )
        except Exception:
            return False

        async with self._state_lock:
            self.counters.quorum_received += 1
            self.clock.update(int(ack.responder_lamport_ts))
        return True

    async def _perform_read_repair(
        self,
        key: str,
        latest: VersionedValue,
        stale_nodes: frozenset[int],
    ) -> set[int]:
        repaired: set[int] = set()
        if self.node_id in stale_nodes:
            async with self._state_lock:
                self.replica_store.put_if_newer(key, latest)
            repaired.add(self.node_id)

        remote_stale = [pid for pid in stale_nodes if pid != self.node_id]
        if not remote_stale:
            return repaired

        async with self._state_lock:
            sender_ts = self.clock.tick()

        results = await asyncio.gather(
            *(
                self._write_single_replica(pid, key, latest, sender_ts, True)
                for pid in remote_stale
            ),
            return_exceptions=True,
        )
        for pid, ok in zip(remote_stale, results, strict=True):
            if ok is True:
                repaired.add(pid)
        return repaired

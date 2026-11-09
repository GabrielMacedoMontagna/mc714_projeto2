"""Servidor e Servicer gRPC assíncrono (grpc.aio) para o nó distribuído."""

from __future__ import annotations

import time

import grpc
import grpc.aio

from mc714_p2.domain.quorum import VersionedValue
from mc714_p2.node.config import NodeSettings
from mc714_p2.node.coordinator import DistributedNode
from mc714_p2.rpc.proto_gen import cluster_pb2, cluster_pb2_grpc


class ClusterNodeServicer(cluster_pb2_grpc.ClusterNodeServiceServicer):
    """Adaptador gRPC que expõe os protocolos distribuídos e endpoints de controle."""

    def __init__(self, node: DistributedNode) -> None:
        self.node = node

    async def SendHeartbeat(  # noqa: N802
        self,
        request: cluster_pb2.HeartbeatMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.HeartbeatAck:
        if not await self.node.check_inbound_fault(int(request.sender_id), context):
            return cluster_pb2.HeartbeatAck()
        reply_ts, accepted = await self.node.handle_heartbeat(
            sender_id=int(request.sender_id),
            sequence=int(request.sequence),
            sender_lamport_ts=int(request.lamport_ts),
        )
        return cluster_pb2.HeartbeatAck(
            responder_id=self.node.node_id,
            sequence=request.sequence,
            lamport_ts=reply_ts,
            accepted=accepted,
        )

    async def RequestAccess(  # noqa: N802
        self,
        request: cluster_pb2.MutexRequestMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.MutexRequestAck:
        if not await self.node.check_inbound_fault(int(request.requester_id), context):
            return cluster_pb2.MutexRequestAck()
        granted, reply_ts = await self.node.handle_mutex_request(
            requester_id=int(request.requester_id),
            request_lamport_ts=int(request.request_lamport_ts),
        )
        return cluster_pb2.MutexRequestAck(
            responder_id=self.node.node_id,
            granted_immediately=granted,
            responder_lamport_ts=reply_ts,
        )

    async def DeliverDeferredReply(  # noqa: N802
        self,
        request: cluster_pb2.MutexReplyMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.MutexAck:
        if not await self.node.check_inbound_fault(int(request.responder_id), context):
            return cluster_pb2.MutexAck()
        ok = await self.node.handle_deferred_reply(
            responder_id=int(request.responder_id),
            responder_lamport_ts=int(request.responder_lamport_ts),
        )
        return cluster_pb2.MutexAck(ok=ok)

    async def ReplicaRead(  # noqa: N802
        self,
        request: cluster_pb2.ReplicaReadMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.ReplicaReadAck:
        if not await self.node.check_inbound_fault(
            int(request.coordinator_id),
            context,
        ):
            return cluster_pb2.ReplicaReadAck()
        val, resp_ts = await self.node.handle_replica_read(
            key=request.key,
            sender_lamport_ts=int(request.lamport_ts),
        )
        return cluster_pb2.ReplicaReadAck(
            replica_id=self.node.node_id,
            key=request.key,
            value=val.value,
            version=val.version,
            writer_id=val.writer_id,
            value_lamport_ts=val.lamport_ts,
            responder_lamport_ts=resp_ts,
            found=val.found,
        )

    async def ReplicaWrite(  # noqa: N802
        self,
        request: cluster_pb2.ReplicaWriteMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.ReplicaWriteAck:
        if not await self.node.check_inbound_fault(
            int(request.coordinator_id),
            context,
        ):
            return cluster_pb2.ReplicaWriteAck()
        candidate = VersionedValue(
            value=request.value,
            version=int(request.version),
            writer_id=int(request.writer_id),
            lamport_ts=int(request.value_lamport_ts),
            updated_wall_ns=time.time_ns(),
        )
        applied, current, resp_ts = await self.node.handle_replica_write(
            key=request.key,
            candidate=candidate,
            sender_lamport_ts=int(request.sender_lamport_ts),
        )
        return cluster_pb2.ReplicaWriteAck(
            replica_id=self.node.node_id,
            applied=applied,
            current_version=current.version,
            current_writer_id=current.writer_id,
            responder_lamport_ts=resp_ts,
        )

    async def ClientExecuteCriticalSection(  # noqa: N802
        self,
        request: cluster_pb2.ClientCSRequest,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.ClientCSReply:
        _ = context
        outcome = await self.node.execute_critical_section(
            resource_tag=request.resource_tag or "default",
            hold_ms=float(request.hold_ms),
            timeout_ms=float(request.timeout_ms) if request.timeout_ms > 0 else 3000.0,
            optional_quorum_key=request.optional_quorum_key,
            optional_quorum_value=request.optional_quorum_value,
        )
        return cluster_pb2.ClientCSReply(
            acquired=outcome.acquired,
            node_id=outcome.node_id,
            request_lamport_ts=outcome.request_lamport_ts,
            enter_lamport_ts=outcome.enter_lamport_ts,
            exit_lamport_ts=outcome.exit_lamport_ts,
            enter_wall_ns=outcome.enter_wall_ns,
            exit_wall_ns=outcome.exit_wall_ns,
            wait_latency_ms=outcome.wait_latency_ms,
            protocol_messages_used=outcome.protocol_messages_used,
            error_message=outcome.error_message,
        )

    async def ClientQuorumRead(  # noqa: N802
        self,
        request: cluster_pb2.ClientReadRequest,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.ClientReadReply:
        _ = context
        start_ns = time.time_ns()
        try:
            res = await self.node.quorum_read(
                key=request.key,
                enable_read_repair=bool(request.enable_read_repair),
                override_read_quorum=int(request.override_read_quorum),
            )
            end_ns = time.time_ns()
            return cluster_pb2.ClientReadReply(
                success=True,
                found=res.value.found,
                key=res.key,
                value=res.value.value,
                version=res.value.version,
                writer_id=res.value.writer_id,
                acks_received=res.acks_received,
                quorum_required=res.quorum_required,
                repaired_nodes=sorted(res.stale_nodes_repaired),
                latency_ms=res.latency_ms,
                start_wall_ns=start_ns,
                end_wall_ns=end_ns,
            )
        except Exception as exc:
            end_ns = time.time_ns()
            return cluster_pb2.ClientReadReply(
                success=False,
                found=False,
                key=request.key,
                start_wall_ns=start_ns,
                end_wall_ns=end_ns,
                error_message=str(exc),
            )

    async def ClientQuorumWrite(  # noqa: N802
        self,
        request: cluster_pb2.ClientWriteRequest,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.ClientWriteReply:
        _ = context
        start_ns = time.time_ns()
        try:
            res = await self.node.quorum_write(
                key=request.key,
                value=request.value,
                override_write_quorum=int(request.override_write_quorum),
            )
            end_ns = time.time_ns()
            return cluster_pb2.ClientWriteReply(
                success=True,
                key=res.key,
                value=res.value.value,
                version=res.value.version,
                writer_id=res.value.writer_id,
                read_phase_acks=res.read_phase_acks,
                write_phase_acks=res.write_phase_acks,
                quorum_required=res.quorum_required,
                latency_ms=res.latency_ms,
                start_wall_ns=start_ns,
                end_wall_ns=end_ns,
            )
        except Exception as exc:
            end_ns = time.time_ns()
            return cluster_pb2.ClientWriteReply(
                success=False,
                key=request.key,
                value=request.value,
                start_wall_ns=start_ns,
                end_wall_ns=end_ns,
                error_message=str(exc),
            )

    async def ConfigureFaults(  # noqa: N802
        self,
        request: cluster_pb2.FaultConfigMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.FaultConfigAck:
        _ = context
        _apply_fault_config(self.node, request)
        st = self.node.fault_injector.stats()
        return cluster_pb2.FaultConfigAck(
            ok=True,
            crashed=st.crashed,
            partitioned_peers=sorted(st.partitioned_peers),
            drop_probability=st.drop_probability,
            delay_ms=st.delay_ms,
            jitter_ms=st.jitter_ms,
        )

    async def GetNodeStatus(  # noqa: N802
        self,
        request: cluster_pb2.StatusRequestMsg,
        context: grpc.aio.ServicerContext,
    ) -> cluster_pb2.StatusReplyMsg:
        _ = context
        return _build_status_reply(self.node, include_history=request.include_history)


def _apply_fault_config(
    node: DistributedNode,
    req: cluster_pb2.FaultConfigMsg,
) -> None:
    if req.reset_all:
        seed = int(req.seed) if req.set_seed else node.settings.fault_seed
        node.fault_injector.reset(seed=seed)
    else:
        node.fault_injector.configure(
            crashed=bool(req.crashed) if req.set_crashed else None,
            partitioned_peers=(
                [int(x) for x in req.partitioned_peers] if req.set_partition else None
            ),
            drop_probability=float(req.drop_probability) if req.set_drop else None,
            delay_ms=float(req.delay_ms) if req.set_delay else None,
            jitter_ms=float(req.jitter_ms) if req.set_delay else None,
            seed=int(req.seed) if req.set_seed else None,
        )
    if req.set_hb_timeout and req.hb_timeout_ms > 0:
        node.failure_detector.set_timeout_ms(float(req.hb_timeout_ms))


def _build_status_reply(
    node: DistributedNode,
    include_history: bool,
) -> cluster_pb2.StatusReplyMsg:
    fstats = node.fault_injector.stats()
    peer_health_msgs = [
        cluster_pb2.PeerHealthMsg(
            peer_id=st.peer_id,
            status=st.status.value,
            last_sequence=st.last_sequence,
            suspect_count=st.suspect_count,
        )
        for st in node.failure_detector.peer_states().values()
    ]
    history_msgs = (
        [
            cluster_pb2.CSRecordMsg(
                node_id=rec.node_id,
                request_lamport=rec.request_lamport,
                enter_lamport=rec.enter_lamport,
                exit_lamport=rec.exit_lamport,
                enter_wall_ns=rec.enter_wall_ns,
                exit_wall_ns=rec.exit_wall_ns,
                resource_tag=rec.resource_tag,
            )
            for rec in node.mutex.history
        ]
        if include_history
        else []
    )
    return cluster_pb2.StatusReplyMsg(
        node_id=node.node_id,
        lamport_clock=node.clock.time,
        mutex_state=node.mutex.state.value,
        active_peers=sorted(node.mutex.active_peers),
        suspected_peers=sorted(node.failure_detector.suspected_peers()),
        peer_health=peer_health_msgs,
        replica_keys_count=len(node.replica_store),
        is_crashed=fstats.crashed,
        mutex_messages_sent=node.counters.mutex_sent,
        mutex_messages_received=node.counters.mutex_received,
        quorum_messages_sent=node.counters.quorum_sent,
        quorum_messages_received=node.counters.quorum_received,
        heartbeat_messages_sent=node.counters.heartbeat_sent,
        heartbeat_messages_received=node.counters.heartbeat_received,
        dropped_messages=fstats.dropped_count,
        partition_blocked_messages=fstats.partition_blocked_count,
        cs_history=history_msgs,
    )


class ManagedNodeServer:
    """Empacota DistributedNode e grpc.aio.Server para contêiner ou teste."""

    def __init__(self, settings: NodeSettings) -> None:
        self.settings = settings
        self.node = DistributedNode(settings=settings)
        self.servicer = ClusterNodeServicer(self.node)
        self._server: grpc.aio.Server | None = None

    async def start(self, start_heartbeat: bool = True) -> None:
        """Inicia o servidor gRPC na porta configurada e o loop de heartbeat."""
        server = grpc.aio.server()
        cluster_pb2_grpc.add_ClusterNodeServiceServicer_to_server(
            self.servicer,
            server,
        )
        bind_addr = f"{self.settings.host}:{self.settings.port}"
        bound_port = server.add_insecure_port(bind_addr)
        if bound_port == 0:
            raise RuntimeError(f"Falha ao fazer bind gRPC em {bind_addr}.")
        await server.start()
        self._server = server
        if start_heartbeat:
            await self.node.start_background_tasks()

    async def stop(self, grace: float = 0.2) -> None:
        """Para o loop de heartbeat, fecha canais de cliente e encerra o servidor."""
        await self.node.stop()
        if self._server is not None:
            await self._server.stop(grace)
            self._server = None

    async def wait_for_termination(self) -> None:
        """Bloqueia até que o servidor gRPC seja encerrado."""
        if self._server is not None:
            await self._server.wait_for_termination()

"""Cliente gRPC assíncrono (grpc.aio) com deadlines/timeouts explícitos obrigatórios."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import grpc
import grpc.aio

from mc714_p2.rpc.proto_gen import cluster_pb2, cluster_pb2_grpc


def _validate_timeout(timeout_sec: float) -> float:
    """Garante que toda chamada RPC possua deadline/timeout explícito positivo."""
    if timeout_sec <= 0.0:
        raise ValueError(
            f"Timeouts explícitos (> 0) são obrigatórios em todas as chamadas gRPC "
            f"(recebido: {timeout_sec})."
        )
    return float(timeout_sec)


@dataclass(frozen=True, slots=True)
class PeerEndpoint:
    """Endereço de rede de um nó lógico do cluster."""

    node_id: int
    host: str
    port: int

    @property
    def target(self) -> str:
        return f"{self.host}:{self.port}"


class PeerRpcClient:
    """Pool de canais gRPC assíncronos para comunicação entre nós do cluster."""

    def __init__(self, peers: Mapping[int, PeerEndpoint]) -> None:
        self._peers: dict[int, PeerEndpoint] = dict(peers)
        self._channels: dict[int, grpc.aio.Channel] = {}
        self._stubs: dict[int, cluster_pb2_grpc.ClusterNodeServiceStub] = {}

    @property
    def peer_ids(self) -> frozenset[int]:
        return frozenset(self._peers.keys())

    def _get_stub(self, peer_id: int) -> cluster_pb2_grpc.ClusterNodeServiceStub:
        stub = self._stubs.get(peer_id)
        if stub is not None:
            return stub
        endpoint = self._peers.get(peer_id)
        if endpoint is None:
            raise KeyError(f"Peer {peer_id} não registrado na tabela de endereços.")
        channel = grpc.aio.insecure_channel(endpoint.target)
        stub = cluster_pb2_grpc.ClusterNodeServiceStub(channel)
        self._channels[peer_id] = channel
        self._stubs[peer_id] = stub
        return stub

    async def close(self) -> None:
        """Fecha todos os canais gRPC abertos."""
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()

    async def send_heartbeat(
        self,
        peer_id: int,
        sender_id: int,
        sequence: int,
        lamport_ts: int,
        *,
        timeout_sec: float,
    ) -> cluster_pb2.HeartbeatAck:
        deadline = _validate_timeout(timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.HeartbeatMsg(
            sender_id=sender_id,
            sequence=sequence,
            lamport_ts=lamport_ts,
        )
        return await stub.SendHeartbeat(req, timeout=deadline)

    async def request_access(
        self,
        peer_id: int,
        requester_id: int,
        request_lamport_ts: int,
        resource_tag: str,
        *,
        timeout_sec: float,
    ) -> cluster_pb2.MutexRequestAck:
        deadline = _validate_timeout(timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.MutexRequestMsg(
            requester_id=requester_id,
            request_lamport_ts=request_lamport_ts,
            resource_tag=resource_tag,
        )
        return await stub.RequestAccess(req, timeout=deadline)

    async def deliver_deferred_reply(
        self,
        peer_id: int,
        responder_id: int,
        responder_lamport_ts: int,
        *,
        timeout_sec: float,
    ) -> cluster_pb2.MutexAck:
        deadline = _validate_timeout(timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.MutexReplyMsg(
            responder_id=responder_id,
            responder_lamport_ts=responder_lamport_ts,
        )
        return await stub.DeliverDeferredReply(req, timeout=deadline)

    async def replica_read(
        self,
        peer_id: int,
        coordinator_id: int,
        key: str,
        lamport_ts: int,
        *,
        timeout_sec: float,
    ) -> cluster_pb2.ReplicaReadAck:
        deadline = _validate_timeout(timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.ReplicaReadMsg(
            coordinator_id=coordinator_id,
            key=key,
            lamport_ts=lamport_ts,
        )
        return await stub.ReplicaRead(req, timeout=deadline)

    async def replica_write(
        self,
        peer_id: int,
        coordinator_id: int,
        key: str,
        value: str,
        version: int,
        writer_id: int,
        value_lamport_ts: int,
        sender_lamport_ts: int,
        is_read_repair: bool,
        *,
        timeout_sec: float,
    ) -> cluster_pb2.ReplicaWriteAck:
        deadline = _validate_timeout(timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.ReplicaWriteMsg(
            coordinator_id=coordinator_id,
            key=key,
            value=value,
            version=version,
            writer_id=writer_id,
            value_lamport_ts=value_lamport_ts,
            sender_lamport_ts=sender_lamport_ts,
            is_read_repair=is_read_repair,
        )
        return await stub.ReplicaWrite(req, timeout=deadline)

    async def client_execute_cs(
        self,
        peer_id: int,
        resource_tag: str,
        hold_ms: float,
        timeout_ms: float,
        optional_quorum_key: str = "",
        optional_quorum_value: str = "",
        *,
        rpc_timeout_sec: float,
    ) -> cluster_pb2.ClientCSReply:
        deadline = _validate_timeout(rpc_timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.ClientCSRequest(
            resource_tag=resource_tag,
            hold_ms=hold_ms,
            timeout_ms=timeout_ms,
            optional_quorum_key=optional_quorum_key,
            optional_quorum_value=optional_quorum_value,
        )
        return await stub.ClientExecuteCriticalSection(req, timeout=deadline)

    async def client_quorum_read(
        self,
        peer_id: int,
        key: str,
        enable_read_repair: bool = True,
        override_read_quorum: int = 0,
        *,
        rpc_timeout_sec: float,
    ) -> cluster_pb2.ClientReadReply:
        deadline = _validate_timeout(rpc_timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.ClientReadRequest(
            key=key,
            enable_read_repair=enable_read_repair,
            override_read_quorum=override_read_quorum,
        )
        return await stub.ClientQuorumRead(req, timeout=deadline)

    async def client_quorum_write(
        self,
        peer_id: int,
        key: str,
        value: str,
        override_write_quorum: int = 0,
        *,
        rpc_timeout_sec: float,
    ) -> cluster_pb2.ClientWriteReply:
        deadline = _validate_timeout(rpc_timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.ClientWriteRequest(
            key=key,
            value=value,
            override_write_quorum=override_write_quorum,
        )
        return await stub.ClientQuorumWrite(req, timeout=deadline)

    async def configure_faults(
        self,
        peer_id: int,
        config_msg: cluster_pb2.FaultConfigMsg,
        *,
        rpc_timeout_sec: float,
    ) -> cluster_pb2.FaultConfigAck:
        deadline = _validate_timeout(rpc_timeout_sec)
        stub = self._get_stub(peer_id)
        return await stub.ConfigureFaults(config_msg, timeout=deadline)

    async def get_node_status(
        self,
        peer_id: int,
        include_history: bool = False,
        *,
        rpc_timeout_sec: float,
    ) -> cluster_pb2.StatusReplyMsg:
        deadline = _validate_timeout(rpc_timeout_sec)
        stub = self._get_stub(peer_id)
        req = cluster_pb2.StatusRequestMsg(include_history=include_history)
        return await stub.GetNodeStatus(req, timeout=deadline)

"""Testes de integração fim a fim com múltiplos nós gRPC reais."""

from __future__ import annotations

import asyncio
import socket

import pytest

from mc714_p2.domain.metrics import (
    QuorumOperationRecord,
    verify_mutual_exclusion_safety,
    verify_quorum_consistency,
)
from mc714_p2.domain.mutex import CriticalSectionRecord
from mc714_p2.node.config import NodeSettings
from mc714_p2.node.grpc_servicer import ManagedNodeServer
from mc714_p2.rpc.client import PeerEndpoint, PeerRpcClient
from mc714_p2.rpc.proto_gen import cluster_pb2


def _allocate_free_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    ports: list[int] = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        ports.append(int(s.getsockname()[1]))
        sockets.append(s)
    for s in sockets:
        s.close()
    return ports


async def _start_test_cluster(
    num_nodes: int,
    quorum_r: int,
    quorum_w: int,
    hb_interval_ms: float = 40.0,
    hb_timeout_ms: float = 140.0,
) -> tuple[dict[int, ManagedNodeServer], PeerRpcClient]:
    ports = _allocate_free_ports(num_nodes)
    peers_str = ",".join(f"{i + 1}=127.0.0.1:{ports[i]}" for i in range(num_nodes))
    endpoints = {
        i + 1: PeerEndpoint(node_id=i + 1, host="127.0.0.1", port=ports[i])
        for i in range(num_nodes)
    }

    servers: dict[int, ManagedNodeServer] = {}
    for node_id in range(1, num_nodes + 1):
        settings = NodeSettings(
            node_id=node_id,
            host="127.0.0.1",
            port=ports[node_id - 1],
            peers=peers_str,
            quorum_r=quorum_r,
            quorum_w=quorum_w,
            hb_interval_ms=hb_interval_ms,
            hb_timeout_ms=hb_timeout_ms,
            rpc_timeout_ms=200.0,
            fault_seed=100 + node_id,
        )
        srv = ManagedNodeServer(settings=settings)
        await srv.start(start_heartbeat=False)
        servers[node_id] = srv

    for srv in servers.values():
        await srv.node.start_background_tasks()

    client = PeerRpcClient(endpoints)
    return servers, client


async def _stop_test_cluster(
    servers: dict[int, ManagedNodeServer],
    client: PeerRpcClient,
) -> None:
    await client.close()
    for srv in servers.values():
        await srv.stop(grace=0.05)


@pytest.mark.asyncio
async def test_cluster_ricart_agrawala_normal_and_crash_recovery() -> None:
    """Valida Ricart-Agrawala + Detector de Falhas em cluster gRPC de 3 nós."""
    servers, client = await _start_test_cluster(
        num_nodes=3,
        quorum_r=2,
        quorum_w=2,
    )
    try:
        # 1. Cenário Normal: disputa concorrente pela Seção Crítica nos 3 nós
        replies = await asyncio.gather(
            *(
                client.client_execute_cs(
                    peer_id=nid,
                    resource_tag="lock-A",
                    hold_ms=15.0,
                    timeout_ms=2000.0,
                    rpc_timeout_sec=3.0,
                )
                for nid in (1, 2, 3, 1, 2, 3)
            )
        )
        assert all(r.acquired for r in replies)

        # 2. Cenário de Falha: Nó 3 sofre crash-stop
        await client.configure_faults(
            peer_id=3,
            config_msg=cluster_pb2.FaultConfigMsg(set_crashed=True, crashed=True),
            rpc_timeout_sec=1.0,
        )
        # Aguarda detecção por heartbeat (~160ms)
        await asyncio.sleep(0.22)

        st1 = await client.get_node_status(peer_id=1, rpc_timeout_sec=1.0)
        assert 3 in st1.suspected_peers

        # Nós 1 e 2 continuam entrando na SC mesmo com o Nó 3 em crash-stop (Liveness!)
        post_crash_replies = await asyncio.gather(
            client.client_execute_cs(
                peer_id=1,
                resource_tag="lock-A",
                hold_ms=10.0,
                timeout_ms=2000.0,
                rpc_timeout_sec=3.0,
            ),
            client.client_execute_cs(
                peer_id=2,
                resource_tag="lock-A",
                hold_ms=10.0,
                timeout_ms=2000.0,
                rpc_timeout_sec=3.0,
            ),
        )
        assert all(r.acquired for r in post_crash_replies)

        # 3. Recuperação: Nó 3 se recupera e volta a participar
        await client.configure_faults(
            peer_id=3,
            config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
            rpc_timeout_sec=1.0,
        )
        await asyncio.sleep(0.12)
        rec_reply = await client.client_execute_cs(
            peer_id=3,
            resource_tag="lock-A",
            hold_ms=10.0,
            timeout_ms=2000.0,
            rpc_timeout_sec=3.0,
        )
        assert rec_reply.acquired

        # Coleta todo o histórico dos 3 nós e comprova Safety (Exclusão Mútua estrita)
        all_records: list[CriticalSectionRecord] = []
        for nid in (1, 2, 3):
            st = await client.get_node_status(
                peer_id=nid,
                include_history=True,
                rpc_timeout_sec=1.0,
            )
            for item in st.cs_history:
                all_records.append(
                    CriticalSectionRecord(
                        node_id=int(item.node_id),
                        request_lamport=int(item.request_lamport),
                        enter_lamport=int(item.enter_lamport),
                        exit_lamport=int(item.exit_lamport),
                        enter_wall_ns=int(item.enter_wall_ns),
                        exit_wall_ns=int(item.exit_wall_ns),
                        resource_tag=item.resource_tag,
                    )
                )
        safety = verify_mutual_exclusion_safety(all_records)
        assert safety.passed, f"Violações de Safety: {safety.violations}"
        assert safety.total_events == 9
    finally:
        await _stop_test_cluster(servers, client)


@pytest.mark.asyncio
async def test_cluster_quorum_replication_partition_and_read_repair() -> None:
    """Valida Replicação por Quorum (N=5, N_R=3, N_W=3), partição 3x2 e Read-Repair."""
    servers, client = await _start_test_cluster(
        num_nodes=5,
        quorum_r=3,
        quorum_w=3,
    )
    try:
        ops_log: list[QuorumOperationRecord] = []

        # 1. Escrita inicial no cluster íntegro via Nó 1
        w1 = await client.client_quorum_write(
            peer_id=1,
            key="conta:100",
            value="saldo=500",
            rpc_timeout_sec=2.0,
        )
        assert w1.success and w1.version == 1
        ops_log.append(
            QuorumOperationRecord(
                "WRITE",
                1,
                w1.key,
                w1.value,
                int(w1.version),
                int(w1.writer_id),
                int(w1.start_wall_ns),
                int(w1.end_wall_ns),
                True,
            )
        )

        # 2. Injetar Partição de Rede 3x2: {1, 2, 3} isolados de {4, 5}
        for maj in (1, 2, 3):
            await client.configure_faults(
                peer_id=maj,
                config_msg=cluster_pb2.FaultConfigMsg(
                    set_partition=True,
                    partitioned_peers=[4, 5],
                ),
                rpc_timeout_sec=1.0,
            )
        for mino in (4, 5):
            await client.configure_faults(
                peer_id=mino,
                config_msg=cluster_pb2.FaultConfigMsg(
                    set_partition=True,
                    partitioned_peers=[1, 2, 3],
                ),
                rpc_timeout_sec=1.0,
            )

        # Partição majoritária {1, 2, 3} consegue escrever v=2 com N_W=3!
        w2 = await client.client_quorum_write(
            peer_id=2,
            key="conta:100",
            value="saldo=750",
            rpc_timeout_sec=2.0,
        )
        assert w2.success and w2.version == 2 and w2.write_phase_acks == 3
        ops_log.append(
            QuorumOperationRecord(
                "WRITE",
                2,
                w2.key,
                w2.value,
                int(w2.version),
                int(w2.writer_id),
                int(w2.start_wall_ns),
                int(w2.end_wall_ns),
                True,
            )
        )

        # Partição minoritária {4, 5} NÃO atinge quórum N_W=3 (previne split-brain!)
        w_minor = await client.client_quorum_write(
            peer_id=4,
            key="conta:100",
            value="saldo=9999",
            rpc_timeout_sec=2.0,
        )
        assert not w_minor.success

        # 3. Cura da partição e leitura com Read-Repair a partir do Nó 4
        for nid in range(1, 6):
            await client.configure_faults(
                peer_id=nid,
                config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
                rpc_timeout_sec=1.0,
            )

        r_healed = await client.client_quorum_read(
            peer_id=4,
            key="conta:100",
            enable_read_repair=True,
            rpc_timeout_sec=2.0,
        )
        assert r_healed.success
        assert r_healed.value == "saldo=750"
        assert r_healed.version == 2
        assert set(r_healed.repaired_nodes) == {4, 5}
        ops_log.append(
            QuorumOperationRecord(
                "READ",
                4,
                r_healed.key,
                r_healed.value,
                int(r_healed.version),
                int(r_healed.writer_id),
                int(r_healed.start_wall_ns),
                int(r_healed.end_wall_ns),
                True,
            )
        )

        consistency = verify_quorum_consistency(ops_log)
        assert consistency.passed
    finally:
        await _stop_test_cluster(servers, client)

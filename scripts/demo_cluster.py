#!/usr/bin/env python3
"""Script de demonstração ao vivo para o cluster Docker Compose (5 nós).

Executa e exibe no terminal:
1. Cenário Normal: Exclusão Mútua (Ricart-Agrawala) e Escrita/Leitura por Quorum;
2. Cenário de Falha Injetada: Crash-Stop do Nó 5 e Partição 3x2 ({1,2,3} vs {4,5});
3. Recuperação e Consequência Esperada: Detecção via Heartbeat, continuidade da Seção
   Crítica e reparo automático de réplicas desatualizadas (Read-Repair).
"""

from __future__ import annotations

import argparse
import asyncio

from mc714_p2.node.config import parse_peers_spec
from mc714_p2.rpc.client import PeerRpcClient
from mc714_p2.rpc.proto_gen import cluster_pb2

DEFAULT_COMPOSE_PEERS = (
    "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053,"
    "4=127.0.0.1:50054,5=127.0.0.1:50055"
)


async def run_demo(peers_spec: str) -> None:
    endpoints = parse_peers_spec(peers_spec)
    client = PeerRpcClient(endpoints)
    try:
        print("=== [1/4] Verificando status inicial dos 5 nós do cluster ===")
        for nid in sorted(endpoints):
            await client.configure_faults(
                peer_id=nid,
                config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
                rpc_timeout_sec=2.0,
            )
        await asyncio.sleep(0.30)
        for nid in sorted(endpoints):
            st = await client.get_node_status(peer_id=nid, rpc_timeout_sec=2.0)
            print(
                f"  -> Nó {st.node_id}: Lamport={st.lamport_clock}, "
                f"Mutex={st.mutex_state}, Peers Ativos={list(st.active_peers)}"
            )

        print(
            "\n=== [2/4] Cenário Normal: Disputa Concorrente pela Seção Crítica "
            "(Ricart-Agrawala) e Escrita Coordenada por Quorum (R=3, W=3) ==="
        )
        cs_replies = await asyncio.gather(
            client.client_execute_cs(
                peer_id=1,
                resource_tag="recurso-compartilhado",
                hold_ms=25.0,
                timeout_ms=3000.0,
                optional_quorum_key="config:lider",
                optional_quorum_value="atualizado-por-no-1",
                rpc_timeout_sec=4.0,
            ),
            client.client_execute_cs(
                peer_id=2,
                resource_tag="recurso-compartilhado",
                hold_ms=25.0,
                timeout_ms=3000.0,
                optional_quorum_key="config:lider",
                optional_quorum_value="atualizado-por-no-2",
                rpc_timeout_sec=4.0,
            ),
            client.client_execute_cs(
                peer_id=3,
                resource_tag="recurso-compartilhado",
                hold_ms=25.0,
                timeout_ms=3000.0,
                optional_quorum_key="config:lider",
                optional_quorum_value="atualizado-por-no-3",
                rpc_timeout_sec=4.0,
            ),
        )
        for rep in sorted(cs_replies, key=lambda r: r.enter_lamport_ts):
            msgs = rep.protocol_messages_used
            print(
                f"  [SC OK] Nó {rep.node_id} | Req={rep.request_lamport_ts} -> "
                f"Intervalo=[{rep.enter_lamport_ts}, {rep.exit_lamport_ts}] | "
                f"Espera={rep.wait_latency_ms:.2f}ms | Msgs={msgs}"
            )

        print(
            "\n=== [3/4] Cenário de Falha Injetada: Crash-Stop no Nó 5 e "
            "Partição de Rede no Nó 4 ==="
        )
        await client.configure_faults(
            peer_id=5,
            config_msg=cluster_pb2.FaultConfigMsg(set_crashed=True, crashed=True),
            rpc_timeout_sec=2.0,
        )
        await client.configure_faults(
            peer_id=4,
            config_msg=cluster_pb2.FaultConfigMsg(
                set_partition=True,
                partitioned_peers=[1, 2, 3, 5],
            ),
            rpc_timeout_sec=2.0,
        )
        for maj in (1, 2, 3):
            await client.configure_faults(
                peer_id=maj,
                config_msg=cluster_pb2.FaultConfigMsg(
                    set_partition=True,
                    partitioned_peers=[4],
                ),
                rpc_timeout_sec=2.0,
            )

        await asyncio.sleep(0.45)
        st1 = await client.get_node_status(peer_id=1, rpc_timeout_sec=2.0)
        print(
            f"  -> Detector de Falhas no Nó 1 identificou nós suspeitos: "
            f"{list(st1.suspected_peers)} (Ativos restantes: {list(st1.active_peers)})"
        )

        w_maj = await client.client_quorum_write(
            peer_id=1,
            key="config:lider",
            value="valor-pos-falha-quorum-majoritario",
            rpc_timeout_sec=3.0,
        )
        print(
            f"  -> Escrita na partição majoritária {{1,2,3}}: "
            f"sucesso={w_maj.success}, v={w_maj.version}, "
            f"ACKs={w_maj.write_phase_acks}/3"
        )

        w_min = await client.client_quorum_write(
            peer_id=4,
            key="config:lider",
            value="tentativa-invalida-particao-minoritaria",
            rpc_timeout_sec=3.0,
        )
        print(
            f"  -> Escrita na partição minoritária isolada {{4}}: "
            f"sucesso={w_min.success} (Bloqueada com segurança: {w_min.error_message})"
        )

        print("\n=== [4/4] Recuperação dos Nós 4 e 5 + Read-Repair Automático ===")
        for nid in sorted(endpoints):
            await client.configure_faults(
                peer_id=nid,
                config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
                rpc_timeout_sec=2.0,
            )
        await asyncio.sleep(0.25)

        r_repair = await client.client_quorum_read(
            peer_id=1,
            key="config:lider",
            enable_read_repair=True,
            rpc_timeout_sec=3.0,
        )
        print(
            f"  -> Leitura por Quórum: valor='{r_repair.value}', "
            f"v={r_repair.version}, ACKs={r_repair.acks_received}, "
            f"Réplicas Reparadas={list(r_repair.repaired_nodes)}"
        )
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demonstração ao vivo do cluster MC714."
    )
    parser.add_argument("--peers", default=DEFAULT_COMPOSE_PEERS)
    args = parser.parse_args()
    asyncio.run(run_demo(args.peers))


if __name__ == "__main__":
    main()

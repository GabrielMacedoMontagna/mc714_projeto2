"""Orquestrador automatizado e reproduzível dos experimentos dos 3 algoritmos."""

from __future__ import annotations

import asyncio
import os
import platform
import random
import socket
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import grpc
from google.protobuf import __version__ as protobuf_version

from mc714_p2.domain.metrics import (
    LatencySummary,
    QuorumOperationRecord,
    summarize_latencies,
    verify_mutual_exclusion_safety,
    verify_quorum_consistency,
)
from mc714_p2.domain.mutex import CriticalSectionRecord
from mc714_p2.node.config import NodeSettings
from mc714_p2.node.grpc_servicer import ManagedNodeServer
from mc714_p2.rpc.client import PeerEndpoint, PeerRpcClient
from mc714_p2.rpc.proto_gen import cluster_pb2


@dataclass(frozen=True, slots=True)
class EnvironmentMetadata:
    """Metadados de ambiente, hardware, SO e parâmetros de execução."""

    timestamp_utc: str
    os_system: str
    os_release: str
    architecture: str
    cpu_count: int
    python_version: str
    grpcio_version: str
    protobuf_version: str
    replicas_per_config: int
    warmup_operations: int
    measured_operations: int
    seeds: list[int]
    total_duration_seconds: float


@dataclass(frozen=True, slots=True)
class MutexExperimentRow:
    """Resultado agregado de uma configuração do Experimento 1 (Ricart-Agrawala)."""

    scenario: str
    num_nodes: int
    network_delay_ms: float
    contending_clients: int
    replicas: int
    warmup_ops: int
    measured_ops_total: int
    latency: LatencySummary
    throughput_ops_sec: float
    mean_messages_per_cs: float
    theoretical_messages_per_cs: float
    safety_passed: bool
    safety_violations: int
    recovery_time_ms: float


@dataclass(frozen=True, slots=True)
class QuorumExperimentRow:
    """Resultado agregado de uma configuração do Experimento 2 (Quorum Gifford)."""

    scenario: str
    num_nodes: int
    quorum_r: int
    quorum_w: int
    network_delay_ms: float
    failed_nodes: int
    replicas: int
    warmup_ops: int
    measured_ops_total: int
    write_latency: LatencySummary
    read_latency: LatencySummary
    write_success_rate: float
    read_success_rate: float
    mean_messages_per_write: float
    mean_messages_per_read: float
    read_repairs_triggered: int
    consistency_passed: bool


@dataclass(frozen=True, slots=True)
class HeartbeatExperimentRow:
    """Resultado agregado de uma configuração do Experimento 3 (Heartbeat)."""

    scenario: str
    num_nodes: int
    hb_interval_ms: float
    hb_timeout_ms: float
    drop_probability: float
    replicas: int
    detection_latency: LatencySummary
    false_positive_rate: float
    measured_msg_per_sec: float
    theoretical_msg_per_sec: float


@dataclass(frozen=True, slots=True)
class FullExperimentSuiteResult:
    """Pacote completo de resultados dos experimentos para exportação e gráficos."""

    metadata: EnvironmentMetadata
    mutex_results: list[MutexExperimentRow]
    quorum_results: list[QuorumExperimentRow]
    heartbeat_results: list[HeartbeatExperimentRow]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _alloc_ports(count: int) -> list[int]:
    socks: list[socket.socket] = []
    ports: list[int] = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        ports.append(int(s.getsockname()[1]))
        socks.append(s)
    for s in socks:
        s.close()
    return ports


async def _spawn_cluster(
    num_nodes: int,
    quorum_r: int,
    quorum_w: int,
    hb_interval_ms: float = 40.0,
    hb_timeout_ms: float = 160.0,
    seed_base: int = 1000,
) -> tuple[dict[int, ManagedNodeServer], PeerRpcClient]:
    ports = _alloc_ports(num_nodes)
    peers_str = ",".join(f"{i + 1}=127.0.0.1:{ports[i]}" for i in range(num_nodes))
    endpoints = {
        i + 1: PeerEndpoint(node_id=i + 1, host="127.0.0.1", port=ports[i])
        for i in range(num_nodes)
    }
    servers: dict[int, ManagedNodeServer] = {}
    for nid in range(1, num_nodes + 1):
        settings = NodeSettings(
            node_id=nid,
            host="127.0.0.1",
            port=ports[nid - 1],
            peers=peers_str,
            quorum_r=quorum_r,
            quorum_w=quorum_w,
            hb_interval_ms=hb_interval_ms,
            hb_timeout_ms=hb_timeout_ms,
            rpc_timeout_ms=250.0,
            fault_seed=seed_base + nid,
        )
        srv = ManagedNodeServer(settings=settings)
        await srv.start(start_heartbeat=False)
        servers[nid] = srv

    for srv in servers.values():
        await srv.node.start_background_tasks()

    return servers, PeerRpcClient(endpoints)


async def _teardown_cluster(
    servers: dict[int, ManagedNodeServer],
    client: PeerRpcClient,
) -> None:
    await client.close()
    for srv in servers.values():
        await srv.stop(grace=0.02)


async def _collect_cluster_cs_records(
    client: PeerRpcClient,
    num_nodes: int,
) -> list[CriticalSectionRecord]:
    records: list[CriticalSectionRecord] = []
    for nid in range(1, num_nodes + 1):
        try:
            st = await client.get_node_status(
                peer_id=nid,
                include_history=True,
                rpc_timeout_sec=1.0,
            )
        except Exception:
            continue
        for item in st.cs_history:
            records.append(
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
    return records


async def run_mutex_configuration(
    *,
    scenario: str,
    num_nodes: int,
    network_delay_ms: float,
    contending_clients: int,
    inject_crash: bool,
    seeds: list[int],
    warmup_ops: int,
    measured_ops: int,
) -> MutexExperimentRow:
    """Executa múltiplas réplicas para uma configuração de Ricart-Agrawala."""
    latencies_ms: list[float] = []
    msg_counts: list[float] = []
    recovery_times_ms: list[float] = []
    total_wall_sec = 0.0
    all_safety_passed = True
    total_violations = 0

    q_maj = (num_nodes // 2) + 1
    for seed in seeds:
        rng = random.Random(seed)
        servers, client = await _spawn_cluster(
            num_nodes=num_nodes,
            quorum_r=q_maj,
            quorum_w=q_maj,
            seed_base=seed,
        )
        try:
            if network_delay_ms > 0.0:
                await _set_cluster_delay(client, num_nodes, network_delay_ms, seed)

            # Warm-up (descartado das estatísticas)
            for _ in range(warmup_ops):
                target = rng.randint(1, num_nodes)
                await client.client_execute_cs(
                    peer_id=target,
                    resource_tag="exp-lock",
                    hold_ms=1.0,
                    timeout_ms=2000.0,
                    rpc_timeout_sec=2.5,
                )

            if inject_crash:
                rec_ms = await _inject_crash_and_measure_recovery(
                    client=client,
                    crashed_node=num_nodes,
                    observer_node=1,
                )
                recovery_times_ms.append(rec_ms)

            active_limit = num_nodes - 1 if inject_crash else num_nodes
            t0 = time.perf_counter()
            ops_lat, ops_msgs = await _execute_mutex_workload(
                client=client,
                active_limit=active_limit,
                contending_clients=contending_clients,
                measured_ops=measured_ops,
                rng=rng,
            )
            total_wall_sec += max(1e-6, time.perf_counter() - t0)
            latencies_ms.extend(ops_lat)
            msg_counts.extend(ops_msgs)

            records = await _collect_cluster_cs_records(client, num_nodes)
            chk = verify_mutual_exclusion_safety(records)
            if not chk.passed:
                all_safety_passed = False
                total_violations += len(chk.violations)
        finally:
            await _teardown_cluster(servers, client)

    lat_summary = summarize_latencies(latencies_ms)
    mean_msgs = sum(msg_counts) / len(msg_counts) if msg_counts else 0.0
    effective_n = num_nodes - 1 if inject_crash else num_nodes
    theoretical_msgs = float(2 * (effective_n - 1))
    mean_recovery = (
        sum(recovery_times_ms) / len(recovery_times_ms) if recovery_times_ms else 0.0
    )

    return MutexExperimentRow(
        scenario=scenario,
        num_nodes=num_nodes,
        network_delay_ms=network_delay_ms,
        contending_clients=contending_clients,
        replicas=len(seeds),
        warmup_ops=warmup_ops,
        measured_ops_total=len(latencies_ms),
        latency=lat_summary,
        throughput_ops_sec=len(latencies_ms) / max(1e-6, total_wall_sec),
        mean_messages_per_cs=mean_msgs,
        theoretical_messages_per_cs=theoretical_msgs,
        safety_passed=all_safety_passed,
        safety_violations=total_violations,
        recovery_time_ms=mean_recovery,
    )


async def _set_cluster_delay(
    client: PeerRpcClient,
    num_nodes: int,
    delay_ms: float,
    seed: int,
) -> None:
    for nid in range(1, num_nodes + 1):
        await client.configure_faults(
            peer_id=nid,
            config_msg=cluster_pb2.FaultConfigMsg(
                set_delay=True,
                delay_ms=delay_ms,
                jitter_ms=delay_ms * 0.2,
                set_seed=True,
                seed=seed + nid,
            ),
            rpc_timeout_sec=1.0,
        )


async def _inject_crash_and_measure_recovery(
    client: PeerRpcClient,
    crashed_node: int,
    observer_node: int,
) -> float:
    t_crash = time.perf_counter()
    await client.configure_faults(
        peer_id=crashed_node,
        config_msg=cluster_pb2.FaultConfigMsg(set_crashed=True, crashed=True),
        rpc_timeout_sec=1.0,
    )
    # Dispara aquisição de SC que precisa detectar a falha para destravar
    reply = await client.client_execute_cs(
        peer_id=observer_node,
        resource_tag="exp-lock",
        hold_ms=1.0,
        timeout_ms=2500.0,
        rpc_timeout_sec=3.0,
    )
    _ = reply
    return (time.perf_counter() - t_crash) * 1000.0


async def _execute_mutex_workload(
    client: PeerRpcClient,
    active_limit: int,
    contending_clients: int,
    measured_ops: int,
    rng: random.Random,
) -> tuple[list[float], list[float]]:
    latencies: list[float] = []
    messages: list[float] = []
    batches = max(1, measured_ops // max(1, contending_clients))

    for _ in range(batches):
        chosen_nodes = [
            ((i + rng.randint(0, active_limit - 1)) % active_limit) + 1
            for i in range(contending_clients)
        ]
        replies = await asyncio.gather(
            *(
                client.client_execute_cs(
                    peer_id=nid,
                    resource_tag="exp-lock",
                    hold_ms=1.5,
                    timeout_ms=2500.0,
                    rpc_timeout_sec=3.0,
                )
                for nid in chosen_nodes
            )
        )
        for rep in replies:
            if rep.acquired:
                latencies.append(float(rep.wait_latency_ms))
                messages.append(float(rep.protocol_messages_used))
    return latencies, messages


async def _set_heterogeneous_topology_delay(
    client: PeerRpcClient,
    num_nodes: int,
    seed: int,
) -> None:
    """Aplica gradiente realista de RTT entre réplicas (ex.: multi-AZ 0..6 ms)."""
    for nid in range(1, num_nodes + 1):
        node_delay = float(nid - 1) * 1.5
        await client.configure_faults(
            peer_id=nid,
            config_msg=cluster_pb2.FaultConfigMsg(
                set_delay=True,
                delay_ms=node_delay,
                jitter_ms=node_delay * 0.1,
                set_seed=True,
                seed=seed + nid,
            ),
            rpc_timeout_sec=1.0,
        )


async def _recover_failed_nodes(
    client: PeerRpcClient,
    num_nodes: int,
    failed_nodes: int,
) -> None:
    for f_idx in range(failed_nodes):
        crash_id = num_nodes - f_idx
        await client.configure_faults(
            peer_id=crash_id,
            config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
            rpc_timeout_sec=1.0,
        )


async def _prepare_quorum_replica_faults(
    client: PeerRpcClient,
    num_nodes: int,
    network_delay_ms: float,
    failed_nodes: int,
    warmup_ops: int,
    seed: int,
) -> None:
    if network_delay_ms > 0.0:
        await _set_cluster_delay(client, num_nodes, network_delay_ms, seed)
    elif failed_nodes == 0:
        await _set_heterogeneous_topology_delay(client, num_nodes, seed)

    for w_idx in range(warmup_ops):
        await client.client_quorum_write(
            peer_id=1,
            key="warmup-k",
            value=f"v-{w_idx}",
            rpc_timeout_sec=2.0,
        )

    for f_idx in range(failed_nodes):
        crash_id = num_nodes - f_idx
        await client.configure_faults(
            peer_id=crash_id,
            config_msg=cluster_pb2.FaultConfigMsg(set_crashed=True, crashed=True),
            rpc_timeout_sec=1.0,
        )


async def run_quorum_configuration(
    *,
    scenario: str,
    num_nodes: int,
    quorum_r: int,
    quorum_w: int,
    network_delay_ms: float,
    failed_nodes: int,
    seeds: list[int],
    warmup_ops: int,
    measured_ops: int,
) -> QuorumExperimentRow:
    """Executa múltiplas réplicas para uma configuração de Replicação por Quorum."""
    write_lats: list[float] = []
    read_lats: list[float] = []
    write_successes = 0
    write_attempts = 0
    read_successes = 0
    read_attempts = 0
    repairs_total = 0
    consistency_ok = True

    for seed in seeds:
        rng = random.Random(seed)
        servers, client = await _spawn_cluster(
            num_nodes=num_nodes,
            quorum_r=quorum_r,
            quorum_w=quorum_w,
            seed_base=seed,
        )
        try:
            await _prepare_quorum_replica_faults(
                client=client,
                num_nodes=num_nodes,
                network_delay_ms=network_delay_ms,
                failed_nodes=failed_nodes,
                warmup_ops=warmup_ops,
                seed=seed,
            )

            alive_max = max(1, num_nodes - failed_nodes)
            ops_log: list[QuorumOperationRecord] = []

            for op_idx in range(measured_ops):
                coord_w = rng.randint(1, alive_max)
                write_attempts += 1
                w_rep = await client.client_quorum_write(
                    peer_id=coord_w,
                    key="bench:key",
                    value=f"val-{seed}-{op_idx}",
                    rpc_timeout_sec=2.0,
                )
                if w_rep.success:
                    write_successes += 1
                    write_lats.append(float(w_rep.latency_ms))
                    ops_log.append(
                        QuorumOperationRecord(
                            "WRITE",
                            coord_w,
                            w_rep.key,
                            w_rep.value,
                            int(w_rep.version),
                            int(w_rep.writer_id),
                            int(w_rep.start_wall_ns),
                            int(w_rep.end_wall_ns),
                            True,
                        )
                    )

                is_repair_step = failed_nodes > 0 and op_idx == measured_ops - 1
                if is_repair_step:
                    await _recover_failed_nodes(client, num_nodes, failed_nodes)
                coord_r = num_nodes if is_repair_step else rng.randint(1, alive_max)

                read_attempts += 1
                r_rep = await client.client_quorum_read(
                    peer_id=coord_r,
                    key="bench:key",
                    enable_read_repair=(failed_nodes > 0),
                    rpc_timeout_sec=2.0,
                )
                if r_rep.success:
                    read_successes += 1
                    read_lats.append(float(r_rep.latency_ms))
                    repairs_total += len(r_rep.repaired_nodes)
                    ops_log.append(
                        QuorumOperationRecord(
                            "READ",
                            coord_r,
                            r_rep.key,
                            r_rep.value,
                            int(r_rep.version),
                            int(r_rep.writer_id),
                            int(r_rep.start_wall_ns),
                            int(r_rep.end_wall_ns),
                            True,
                        )
                    )

            consistency_ok = (
                consistency_ok and verify_quorum_consistency(ops_log).passed
            )
        finally:
            await _teardown_cluster(servers, client)

    return QuorumExperimentRow(
        scenario=scenario,
        num_nodes=num_nodes,
        quorum_r=quorum_r,
        quorum_w=quorum_w,
        network_delay_ms=network_delay_ms,
        failed_nodes=failed_nodes,
        replicas=len(seeds),
        warmup_ops=warmup_ops,
        measured_ops_total=write_successes + read_successes,
        write_latency=summarize_latencies(write_lats),
        read_latency=summarize_latencies(read_lats),
        write_success_rate=write_successes / max(1, write_attempts),
        read_success_rate=read_successes / max(1, read_attempts),
        mean_messages_per_write=float(4 * max(0, quorum_w - 1)),
        mean_messages_per_read=float(2 * max(0, quorum_r - 1)),
        read_repairs_triggered=repairs_total,
        consistency_passed=consistency_ok,
    )


async def run_heartbeat_configuration(
    *,
    scenario: str,
    num_nodes: int,
    hb_interval_ms: float,
    hb_timeout_ms: float,
    drop_probability: float,
    seeds: list[int],
) -> HeartbeatExperimentRow:
    """Avalia tempo de detecção e taxa de falsos positivos do Detector de Falhas."""
    detection_samples_ms: list[float] = []
    false_positives = 0
    total_healthy_checks = 0
    msg_rates: list[float] = []

    q_maj = (num_nodes // 2) + 1
    for seed in seeds:
        servers, client = await _spawn_cluster(
            num_nodes=num_nodes,
            quorum_r=q_maj,
            quorum_w=q_maj,
            hb_interval_ms=hb_interval_ms,
            hb_timeout_ms=hb_timeout_ms,
            seed_base=seed,
        )
        try:
            if drop_probability > 0.0:
                for nid in range(1, num_nodes + 1):
                    await client.configure_faults(
                        peer_id=nid,
                        config_msg=cluster_pb2.FaultConfigMsg(
                            set_drop=True,
                            drop_probability=drop_probability,
                            set_seed=True,
                            seed=seed + nid,
                        ),
                        rpc_timeout_sec=1.0,
                    )

            # Janela de observação sem crash real para medir falsos positivos
            t_obs_start = time.perf_counter()
            await asyncio.sleep(max(0.28, (hb_timeout_ms * 1.8) / 1000.0))
            obs_elapsed = max(1e-3, time.perf_counter() - t_obs_start)

            total_hb_msgs = 0
            for nid in range(1, num_nodes + 1):
                st_n = await client.get_node_status(peer_id=nid, rpc_timeout_sec=1.0)
                total_hb_msgs += int(st_n.heartbeat_messages_sent)
                for ph in st_n.peer_health:
                    total_healthy_checks += 1
                    if ph.suspect_count > 0 or ph.status == "SUSPECTED":
                        false_positives += 1
            msg_rates.append(total_hb_msgs / obs_elapsed)

            # Injeta crash-stop real no último nó e mede o tempo até detecção
            await client.configure_faults(
                peer_id=num_nodes,
                config_msg=cluster_pb2.FaultConfigMsg(set_crashed=True, crashed=True),
                rpc_timeout_sec=1.0,
            )
            t_crash = time.perf_counter()
            detected_ms = await _poll_until_suspected(
                client=client,
                observer_id=1,
                target_id=num_nodes,
                t_crash=t_crash,
                max_wait_sec=2.0,
            )
            detection_samples_ms.append(detected_ms)
        finally:
            await _teardown_cluster(servers, client)

    theoretical_rate = (num_nodes * (num_nodes - 1)) / (hb_interval_ms / 1000.0)
    return HeartbeatExperimentRow(
        scenario=scenario,
        num_nodes=num_nodes,
        hb_interval_ms=hb_interval_ms,
        hb_timeout_ms=hb_timeout_ms,
        drop_probability=drop_probability,
        replicas=len(seeds),
        detection_latency=summarize_latencies(detection_samples_ms),
        false_positive_rate=false_positives / max(1, total_healthy_checks),
        measured_msg_per_sec=sum(msg_rates) / max(1, len(msg_rates)),
        theoretical_msg_per_sec=theoretical_rate,
    )


async def _poll_until_suspected(
    client: PeerRpcClient,
    observer_id: int,
    target_id: int,
    t_crash: float,
    max_wait_sec: float,
) -> float:
    deadline = t_crash + max_wait_sec
    while time.perf_counter() < deadline:
        st = await client.get_node_status(peer_id=observer_id, rpc_timeout_sec=1.0)
        if target_id in st.suspected_peers:
            return (time.perf_counter() - t_crash) * 1000.0
        await asyncio.sleep(0.015)
    return (time.perf_counter() - t_crash) * 1000.0


async def run_full_experiment_suite(
    seeds: list[int] | None = None,
    warmup_ops: int = 3,
    measured_ops: int = 12,
) -> FullExperimentSuiteResult:
    """Executa toda a bateria experimental dos 3 algoritmos e consolida resultados."""
    exp_seeds = seeds if seeds is not None else [101, 202, 303, 404, 505]
    t_suite_start = time.perf_counter()

    # 1. Experimento Ricart-Agrawala: variação de N (3, 5, 7), atraso e falha
    mutex_rows: list[MutexExperimentRow] = []
    for n_nodes in (3, 5, 7):
        mutex_rows.append(
            await run_mutex_configuration(
                scenario=f"Normal (Sem Contenção, N={n_nodes})",
                num_nodes=n_nodes,
                network_delay_ms=0.0,
                contending_clients=1,
                inject_crash=False,
                seeds=exp_seeds,
                warmup_ops=warmup_ops,
                measured_ops=measured_ops,
            )
        )
        mutex_rows.append(
            await run_mutex_configuration(
                scenario=f"Normal (Alta Contenção, N={n_nodes})",
                num_nodes=n_nodes,
                network_delay_ms=0.0,
                contending_clients=n_nodes,
                inject_crash=False,
                seeds=exp_seeds,
                warmup_ops=warmup_ops,
                measured_ops=measured_ops,
            )
        )

    for delay_ms in (5.0, 15.0, 30.0):
        mutex_rows.append(
            await run_mutex_configuration(
                scenario=f"Atraso de Rede ({int(delay_ms)} ms, N=5)",
                num_nodes=5,
                network_delay_ms=delay_ms,
                contending_clients=3,
                inject_crash=False,
                seeds=exp_seeds,
                warmup_ops=warmup_ops,
                measured_ops=measured_ops,
            )
        )

    mutex_rows.append(
        await run_mutex_configuration(
            scenario="Falha Crash-Stop + Recuperação via Heartbeat (N=5, f=1)",
            num_nodes=5,
            network_delay_ms=0.0,
            contending_clients=3,
            inject_crash=True,
            seeds=exp_seeds,
            warmup_ops=warmup_ops,
            measured_ops=measured_ops,
        )
    )

    # 2. Experimento Replicação por Quorum: variação de (N_R, N_W), atraso e falhas
    quorum_rows: list[QuorumExperimentRow] = []
    for qr, qw, label in (
        (3, 3, "Quórum Simétrico (R=3, W=3)"),
        (2, 4, "Otimizado p/ Leitura (R=2, W=4)"),
        (1, 5, "ROWAA Leitura Rápida (R=1, W=5)"),
    ):
        quorum_rows.append(
            await run_quorum_configuration(
                scenario=f"Normal - {label}",
                num_nodes=5,
                quorum_r=qr,
                quorum_w=qw,
                network_delay_ms=0.0,
                failed_nodes=0,
                seeds=exp_seeds,
                warmup_ops=warmup_ops,
                measured_ops=measured_ops,
            )
        )

    for delay_ms in (5.0, 15.0, 30.0):
        quorum_rows.append(
            await run_quorum_configuration(
                scenario=f"Atraso {int(delay_ms)} ms (R=3, W=3)",
                num_nodes=5,
                quorum_r=3,
                quorum_w=3,
                network_delay_ms=delay_ms,
                failed_nodes=0,
                seeds=exp_seeds,
                warmup_ops=warmup_ops,
                measured_ops=measured_ops,
            )
        )

    for f_nodes in (1, 2):
        quorum_rows.append(
            await run_quorum_configuration(
                scenario=f"Falha Crash-Stop f={f_nodes} + Read-Repair (R=3, W=3)",
                num_nodes=5,
                quorum_r=3,
                quorum_w=3,
                network_delay_ms=0.0,
                failed_nodes=f_nodes,
                seeds=exp_seeds,
                warmup_ops=warmup_ops,
                measured_ops=measured_ops,
            )
        )

    # 3. Experimento Detector de Falhas (Heartbeat): variação de timeout e perda
    hb_rows: list[HeartbeatExperimentRow] = []
    for timeout_ms in (120.0, 220.0, 340.0):
        hb_rows.append(
            await run_heartbeat_configuration(
                scenario=f"Timeout {int(timeout_ms)} ms (Sem Perda)",
                num_nodes=5,
                hb_interval_ms=45.0,
                hb_timeout_ms=timeout_ms,
                drop_probability=0.0,
                seeds=exp_seeds,
            )
        )

    for drop_p in (0.10, 0.25, 0.40):
        hb_rows.append(
            await run_heartbeat_configuration(
                scenario=f"Perda de Pacotes {int(drop_p * 100)}% (Timeout 180 ms)",
                num_nodes=5,
                hb_interval_ms=45.0,
                hb_timeout_ms=180.0,
                drop_probability=drop_p,
                seeds=exp_seeds,
            )
        )

    total_sec = time.perf_counter() - t_suite_start
    uname = platform.uname()
    meta = EnvironmentMetadata(
        timestamp_utc=datetime.now(UTC).isoformat(),
        os_system=uname.system,
        os_release=uname.release,
        architecture=uname.machine,
        cpu_count=os.cpu_count() or 1,
        python_version=sys.version.split()[0],
        grpcio_version=grpc.__version__,
        protobuf_version=protobuf_version,
        replicas_per_config=len(exp_seeds),
        warmup_operations=warmup_ops,
        measured_operations=measured_ops,
        seeds=exp_seeds,
        total_duration_seconds=round(total_sec, 3),
    )
    return FullExperimentSuiteResult(
        metadata=meta,
        mutex_results=mutex_rows,
        quorum_results=quorum_rows,
        heartbeat_results=hb_rows,
    )

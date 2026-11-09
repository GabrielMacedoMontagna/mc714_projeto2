"""Exportação de dados brutos (JSON/CSV), tabelas LaTeX e gráficos científicos."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from mc714_p2.experiments.runner import (
    FullExperimentSuiteResult,
    HeartbeatExperimentRow,
    MutexExperimentRow,
    QuorumExperimentRow,
)


def export_all_artifacts(
    suite: FullExperimentSuiteResult,
    results_dir: Path,
) -> None:
    """Salva dados brutos em results/raw/, tabelas em results/tables/ e figuras."""
    raw_dir = results_dir / "raw"
    tables_dir = results_dir / "tables"
    figures_dir = results_dir / "figures"
    for d in (raw_dir, tables_dir, figures_dir):
        d.mkdir(parents=True, exist_ok=True)

    raw_json_path = raw_dir / "experiments_raw.json"
    raw_json_path.write_text(
        json.dumps(suite.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    _export_mutex_csv(suite.mutex_results, tables_dir / "mutex_summary.csv")
    _export_quorum_csv(suite.quorum_results, tables_dir / "quorum_summary.csv")
    _export_heartbeat_csv(suite.heartbeat_results, tables_dir / "heartbeat_summary.csv")

    _plot_mutex_figure(suite.mutex_results, figures_dir)
    _plot_quorum_figure(suite.quorum_results, figures_dir)
    _plot_heartbeat_figure(suite.heartbeat_results, figures_dir)


def _export_mutex_csv(rows: list[MutexExperimentRow], path: Path) -> None:
    fieldnames = [
        "scenario",
        "num_nodes",
        "network_delay_ms",
        "contending_clients",
        "mean_ms",
        "std_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "throughput_ops_sec",
        "mean_messages_per_cs",
        "theoretical_messages_per_cs",
        "safety_passed",
        "recovery_time_ms",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "scenario": r.scenario,
                    "num_nodes": r.num_nodes,
                    "network_delay_ms": r.network_delay_ms,
                    "contending_clients": r.contending_clients,
                    "mean_ms": round(r.latency.mean_ms, 3),
                    "std_ms": round(r.latency.std_ms, 3),
                    "p50_ms": round(r.latency.p50_ms, 3),
                    "p95_ms": round(r.latency.p95_ms, 3),
                    "p99_ms": round(r.latency.p99_ms, 3),
                    "throughput_ops_sec": round(r.throughput_ops_sec, 2),
                    "mean_messages_per_cs": round(r.mean_messages_per_cs, 2),
                    "theoretical_messages_per_cs": r.theoretical_messages_per_cs,
                    "safety_passed": r.safety_passed,
                    "recovery_time_ms": round(r.recovery_time_ms, 2),
                }
            )


def _export_quorum_csv(rows: list[QuorumExperimentRow], path: Path) -> None:
    fieldnames = [
        "scenario",
        "num_nodes",
        "quorum_r",
        "quorum_w",
        "network_delay_ms",
        "failed_nodes",
        "write_mean_ms",
        "write_p50_ms",
        "write_p95_ms",
        "write_p99_ms",
        "read_mean_ms",
        "read_p50_ms",
        "read_p95_ms",
        "read_p99_ms",
        "write_success_rate",
        "read_success_rate",
        "read_repairs_triggered",
        "consistency_passed",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "scenario": r.scenario,
                    "num_nodes": r.num_nodes,
                    "quorum_r": r.quorum_r,
                    "quorum_w": r.quorum_w,
                    "network_delay_ms": r.network_delay_ms,
                    "failed_nodes": r.failed_nodes,
                    "write_mean_ms": round(r.write_latency.mean_ms, 3),
                    "write_p50_ms": round(r.write_latency.p50_ms, 3),
                    "write_p95_ms": round(r.write_latency.p95_ms, 3),
                    "write_p99_ms": round(r.write_latency.p99_ms, 3),
                    "read_mean_ms": round(r.read_latency.mean_ms, 3),
                    "read_p50_ms": round(r.read_latency.p50_ms, 3),
                    "read_p95_ms": round(r.read_latency.p95_ms, 3),
                    "read_p99_ms": round(r.read_latency.p99_ms, 3),
                    "write_success_rate": round(r.write_success_rate, 3),
                    "read_success_rate": round(r.read_success_rate, 3),
                    "read_repairs_triggered": r.read_repairs_triggered,
                    "consistency_passed": r.consistency_passed,
                }
            )


def _export_heartbeat_csv(rows: list[HeartbeatExperimentRow], path: Path) -> None:
    fieldnames = [
        "scenario",
        "num_nodes",
        "hb_interval_ms",
        "hb_timeout_ms",
        "drop_probability",
        "detect_mean_ms",
        "detect_std_ms",
        "detect_p50_ms",
        "detect_p95_ms",
        "detect_p99_ms",
        "false_positive_rate",
        "measured_msg_per_sec",
        "theoretical_msg_per_sec",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "scenario": r.scenario,
                    "num_nodes": r.num_nodes,
                    "hb_interval_ms": r.hb_interval_ms,
                    "hb_timeout_ms": r.hb_timeout_ms,
                    "drop_probability": r.drop_probability,
                    "detect_mean_ms": round(r.detection_latency.mean_ms, 2),
                    "detect_std_ms": round(r.detection_latency.std_ms, 2),
                    "detect_p50_ms": round(r.detection_latency.p50_ms, 2),
                    "detect_p95_ms": round(r.detection_latency.p95_ms, 2),
                    "detect_p99_ms": round(r.detection_latency.p99_ms, 2),
                    "false_positive_rate": round(r.false_positive_rate, 4),
                    "measured_msg_per_sec": round(r.measured_msg_per_sec, 1),
                    "theoretical_msg_per_sec": round(r.theoretical_msg_per_sec, 1),
                }
            )


def _plot_mutex_figure(rows: list[MutexExperimentRow], figures_dir: Path) -> None:
    contended = [
        r for r in rows if "Alta Contenção" in r.scenario and r.network_delay_ms == 0.0
    ]
    uncontended = [
        r for r in rows if "Sem Contenção" in r.scenario and r.network_delay_ms == 0.0
    ]
    if not contended or not uncontended:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.0))

    nodes = [r.num_nodes for r in contended]
    ax1.plot(
        nodes,
        [r.latency.p50_ms for r in uncontended],
        "o--",
        label="Sem Contenção (p50)",
        color="#1f77b4",
    )
    ax1.plot(
        nodes,
        [r.latency.p50_ms for r in contended],
        "s-",
        label="Alta Contenção (p50)",
        color="#ff7f0e",
    )
    ax1.plot(
        nodes,
        [r.latency.p95_ms for r in contended],
        "^-",
        label="Alta Contenção (p95)",
        color="#d62728",
    )
    ax1.plot(
        nodes,
        [r.latency.p99_ms for r in contended],
        "d:",
        label="Alta Contenção (p99)",
        color="#9467bd",
    )
    ax1.set_title("(a) Latência de Aquisição da SC vs. Número de Nós (N)")
    ax1.set_xlabel("Número de Nós (N)")
    ax1.set_ylabel("Latência de Espera (ms)")
    ax1.set_xticks(nodes)
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend(fontsize=8)

    ax2.bar(
        [n - 0.18 for n in nodes],
        [r.theoretical_messages_per_cs for r in uncontended],
        width=0.35,
        label="Teórico: 2(N - 1)",
        color="#2ca02c",
        alpha=0.85,
    )
    ax2.bar(
        [n + 0.18 for n in nodes],
        [r.mean_messages_per_cs for r in uncontended],
        width=0.35,
        label="Medido (gRPC)",
        color="#1f77b4",
        alpha=0.85,
    )
    ax2.set_title("(b) Complexidade de Mensagens por Entrada na SC")
    ax2.set_xlabel("Número de Nós (N)")
    ax2.set_ylabel("Mensagens Trocadas por Aquisição")
    ax2.set_xticks(nodes)
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(figures_dir / "fig1_mutex_scalability.pdf", bbox_inches="tight")
    fig.savefig(
        figures_dir / "fig1_mutex_scalability.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)


def _plot_quorum_figure(rows: list[QuorumExperimentRow], figures_dir: Path) -> None:
    configs = [r for r in rows if r.scenario.startswith("Normal -")]
    delays = [
        r for r in rows if (r.quorum_r == 3 and r.quorum_w == 3 and r.failed_nodes == 0)
    ]
    if not configs:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.0))

    labels = [f"R={r.quorum_r}, W={r.quorum_w}" for r in configs]
    x_pos = list(range(len(configs)))
    ax1.bar(
        [x - 0.18 for x in x_pos],
        [r.read_latency.p50_ms for r in configs],
        width=0.35,
        label="Leitura (p50)",
        color="#1f77b4",
    )
    ax1.bar(
        [x + 0.18 for x in x_pos],
        [r.write_latency.p50_ms for r in configs],
        width=0.35,
        label="Escrita 2-Fases (p50)",
        color="#ff7f0e",
    )
    ax1.errorbar(
        [x - 0.18 for x in x_pos],
        [r.read_latency.p50_ms for x, r in zip(x_pos, configs, strict=True)],
        yerr=[max(0.0, r.read_latency.p95_ms - r.read_latency.p50_ms) for r in configs],
        fmt="none",
        ecolor="black",
        capsize=4,
    )
    ax1.errorbar(
        [x + 0.18 for x in x_pos],
        [r.write_latency.p50_ms for x, r in zip(x_pos, configs, strict=True)],
        yerr=[
            max(0.0, r.write_latency.p95_ms - r.write_latency.p50_ms) for r in configs
        ],
        fmt="none",
        ecolor="black",
        capsize=4,
    )
    ax1.set_title("(a) Trade-off de Quórum (N=5): Leitura vs. Escrita (p50 + p95)")
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(labels)
    ax1.set_ylabel("Latência (ms)")
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend(fontsize=8)

    delays_sorted = sorted(delays, key=lambda r: r.network_delay_ms)
    d_vals = [r.network_delay_ms for r in delays_sorted]
    ax2.plot(
        d_vals,
        [r.read_latency.p50_ms for r in delays_sorted],
        "o-",
        label="Leitura p50 (1 RTT)",
        color="#1f77b4",
    )
    ax2.plot(
        d_vals,
        [r.read_latency.p95_ms for r in delays_sorted],
        "o--",
        label="Leitura p95",
        color="#17becf",
    )
    ax2.plot(
        d_vals,
        [r.write_latency.p50_ms for r in delays_sorted],
        "s-",
        label="Escrita p50 (2 RTTs)",
        color="#ff7f0e",
    )
    ax2.plot(
        d_vals,
        [r.write_latency.p95_ms for r in delays_sorted],
        "s--",
        label="Escrita p95",
        color="#d62728",
    )
    ax2.set_title("(b) Sensibilidade ao Atraso de Rede (N=5, R=3, W=3)")
    ax2.set_xlabel("Atraso Injetado por Canal (ms)")
    ax2.set_ylabel("Latência de Operação (ms)")
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(figures_dir / "fig2_quorum_tradeoffs.pdf", bbox_inches="tight")
    fig.savefig(
        figures_dir / "fig2_quorum_tradeoffs.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)


def _plot_heartbeat_figure(
    rows: list[HeartbeatExperimentRow],
    figures_dir: Path,
) -> None:
    by_timeout = [r for r in rows if r.drop_probability == 0.0]
    by_drop = [r for r in rows if r.drop_probability > 0.0 or r.hb_timeout_ms == 120.0]
    if not by_timeout:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.0))

    t_vals = [r.hb_timeout_ms for r in by_timeout]
    ax1.plot(
        t_vals,
        [r.detection_latency.p50_ms for r in by_timeout],
        "o-",
        label="Tempo de Detecção (p50)",
        color="#2ca02c",
    )
    ax1.plot(
        t_vals,
        [r.detection_latency.p95_ms for r in by_timeout],
        "s--",
        label="Tempo de Detecção (p95)",
        color="#d62728",
    )
    ax1.plot(
        t_vals,
        t_vals,
        ":",
        label="Limite Teórico (Timeout τ)",
        color="gray",
    )
    ax1.set_title("(a) Tempo de Detecção de Crash vs. Timeout de Suspeita (τ)")
    ax1.set_xlabel("Timeout de Suspeita Configurado τ (ms)")
    ax1.set_ylabel("Latência de Detecção (ms)")
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend(fontsize=8)

    drop_sorted = sorted(by_drop, key=lambda r: r.drop_probability)
    p_vals = [r.drop_probability * 100.0 for r in drop_sorted]
    fp_vals = [r.false_positive_rate * 100.0 for r in drop_sorted]
    ax2.plot(
        p_vals,
        fp_vals,
        "^-",
        color="#d62728",
        linewidth=2,
        label="Falsos Positivos (%)",
    )
    ax2.set_title("(b) Taxa de Falsos Positivos vs. Perda de Pacotes na Rede")
    ax2.set_xlabel("Probabilidade de Perda de Pacotes (%)")
    ax2.set_ylabel("Taxa de Suspeitas Falsas (%)")
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(figures_dir / "fig3_heartbeat_detection.pdf", bbox_inches="tight")
    fig.savefig(
        figures_dir / "fig3_heartbeat_detection.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)

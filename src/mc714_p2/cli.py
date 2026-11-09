"""Interface de linha de comando (CLI) para nós distribuídos e experimentos."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from collections.abc import Sequence
from pathlib import Path

from mc714_p2.experiments.plotting import export_all_artifacts
from mc714_p2.experiments.runner import run_full_experiment_suite
from mc714_p2.node.config import NodeSettings
from mc714_p2.node.grpc_servicer import ManagedNodeServer


def build_parser() -> argparse.ArgumentParser:
    """Constrói o parser de argumentos da CLI mc714-p2."""
    parser = argparse.ArgumentParser(
        prog="mc714-p2",
        description="Nó distribuído e suíte experimental do Trabalho 2 de MC714.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    node_cmd = sub.add_parser("node", help="Inicia um nó gRPC do cluster distribuído.")
    node_cmd.add_argument("--node-id", type=int, default=None)
    node_cmd.add_argument("--host", type=str, default=None)
    node_cmd.add_argument("--port", type=int, default=None)
    node_cmd.add_argument("--peers", type=str, default=None)
    node_cmd.add_argument("--quorum-r", type=int, default=None)
    node_cmd.add_argument("--quorum-w", type=int, default=None)
    node_cmd.add_argument("--hb-interval-ms", type=float, default=None)
    node_cmd.add_argument("--hb-timeout-ms", type=float, default=None)
    node_cmd.add_argument("--rpc-timeout-ms", type=float, default=None)
    node_cmd.add_argument("--fault-seed", type=int, default=None)

    exp_cmd = sub.add_parser(
        "experiments",
        help="Executa a suíte de experimentos automatizados e gera gráficos.",
    )
    exp_cmd.add_argument(
        "--output-dir",
        type=str,
        default="results",
        help="Diretório de saída para dados brutos, tabelas e figuras.",
    )
    exp_cmd.add_argument(
        "--replicas",
        type=int,
        default=5,
        help="Número de réplicas/sementes independentes por configuração.",
    )
    exp_cmd.add_argument(
        "--warmup-ops",
        type=int,
        default=3,
        help="Operações de aquecimento descartadas antes da medição.",
    )
    exp_cmd.add_argument(
        "--measured-ops",
        type=int,
        default=12,
        help="Operações medidas por réplica em cada configuração.",
    )
    return parser


def resolve_node_settings(args: argparse.Namespace) -> NodeSettings:
    """Combina variáveis de ambiente MC714_* com sobrescritas da linha de comando."""
    overrides: dict[str, object] = {}
    for field_name in (
        "node_id",
        "host",
        "port",
        "peers",
        "quorum_r",
        "quorum_w",
        "hb_interval_ms",
        "hb_timeout_ms",
        "rpc_timeout_ms",
        "fault_seed",
    ):
        val = getattr(args, field_name, None)
        if val is not None:
            overrides[field_name] = val
    return NodeSettings(**overrides)  # type: ignore[arg-type]


async def run_node_forever(settings: NodeSettings) -> None:
    """Inicia o servidor gRPC do nó e aguarda sinal de término (SIGINT/SIGTERM)."""
    managed = ManagedNodeServer(settings=settings)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with ExceptionSuppression():
            loop.add_signal_handler(sig, stop_event.set)

    await managed.start(start_heartbeat=True)
    print(
        f"[mc714-p2] Nó {settings.node_id} escutando em "
        f"{settings.host}:{settings.port} (N_R={settings.quorum_r}, "
        f"N_W={settings.quorum_w})",
        flush=True,
    )
    try:
        await stop_event.wait()
    finally:
        await managed.stop()


async def run_experiments_command(args: argparse.Namespace) -> None:
    """Executa todos os experimentos e salva artefatos em disco."""
    seeds = [100 * (i + 1) + 1 for i in range(max(1, int(args.replicas)))]
    suite = await run_full_experiment_suite(
        seeds=seeds,
        warmup_ops=int(args.warmup_ops),
        measured_ops=int(args.measured_ops),
    )
    out_dir = Path(str(args.output_dir))
    export_all_artifacts(suite, out_dir)
    dur = suite.metadata.total_duration_seconds
    print(
        f"[mc714-p2] Experimentos concluídos em {dur}s. "
        f"Artefatos salvos em '{out_dir}/'.",
        flush=True,
    )


class ExceptionSuppression:
    """Context manager simples para ignorar NotImplementedError em sinais."""

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> bool:
        return exc_type in (NotImplementedError, RuntimeError)


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada síncrono do executável mc714-p2."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "node":
        settings = resolve_node_settings(args)
        asyncio.run(run_node_forever(settings))
        return 0
    if args.command == "experiments":
        asyncio.run(run_experiments_command(args))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

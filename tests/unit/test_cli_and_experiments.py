"""Testes da CLI, do orquestrador de experimentos e da exportação de gráficos."""

from __future__ import annotations

from pathlib import Path

import pytest

from mc714_p2.cli import build_parser, resolve_node_settings
from mc714_p2.experiments.plotting import export_all_artifacts
from mc714_p2.experiments.runner import run_full_experiment_suite


def test_cli_parser_and_resolve_node_settings() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "node",
            "--node-id",
            "3",
            "--port",
            "50099",
            "--quorum-r",
            "2",
            "--quorum-w",
            "2",
        ]
    )
    settings = resolve_node_settings(args)
    assert settings.node_id == 3
    assert settings.port == 50099
    assert settings.quorum_r == 2
    assert settings.quorum_w == 2


@pytest.mark.asyncio
async def test_experiment_suite_and_export_artifacts(tmp_path: Path) -> None:
    suite = await run_full_experiment_suite(
        seeds=[701],
        warmup_ops=1,
        measured_ops=2,
    )
    assert len(suite.mutex_results) >= 4
    assert len(suite.quorum_results) >= 4
    assert len(suite.heartbeat_results) >= 4
    assert all(r.safety_passed for r in suite.mutex_results)
    assert all(r.consistency_passed for r in suite.quorum_results)

    export_all_artifacts(suite, tmp_path)
    assert (tmp_path / "raw" / "experiments_raw.json").exists()
    assert (tmp_path / "tables" / "mutex_summary.csv").exists()
    assert (tmp_path / "tables" / "quorum_summary.csv").exists()
    assert (tmp_path / "tables" / "heartbeat_summary.csv").exists()
    assert (tmp_path / "figures" / "fig1_mutex_scalability.pdf").exists()
    assert (tmp_path / "figures" / "fig2_quorum_tradeoffs.pdf").exists()
    assert (tmp_path / "figures" / "fig3_heartbeat_detection.pdf").exists()

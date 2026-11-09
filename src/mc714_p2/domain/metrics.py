"""Métricas de latência (com percentis p50, p95, p99) e verificadores de correção."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from mc714_p2.domain.mutex import CriticalSectionRecord


@dataclass(frozen=True, slots=True)
class LatencySummary:
    """Estatísticas completas de latência exigidas pelo enunciado."""

    count: int
    mean_ms: float
    std_ms: float
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass(frozen=True, slots=True)
class SafetyVerificationResult:
    """Resultado auditável da verificação de Safety e Liveness de um algoritmo."""

    passed: bool
    total_events: int
    violations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class QuorumOperationRecord:
    """Registro de operação de leitura/escrita por quórum para verificação."""

    op_type: str  # "WRITE" ou "READ"
    coordinator_id: int
    key: str
    value: str
    version: int
    writer_id: int
    start_ns: int
    end_ns: int
    success: bool


def compute_percentile(sorted_values: Sequence[float], percentile: float) -> float:
    """Calcula o percentil linear (0 a 100) sobre uma sequência ordenada."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (percentile / 100.0) * (len(sorted_values) - 1)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return float(sorted_values[lower])
    weight = rank - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def summarize_latencies(samples_ms: Sequence[float]) -> LatencySummary:
    """Calcula média, desvio padrão amostral e percentis p50, p95, p99."""
    n = len(samples_ms)
    if n == 0:
        return LatencySummary(
            count=0,
            mean_ms=0.0,
            std_ms=0.0,
            min_ms=0.0,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            max_ms=0.0,
        )
    ordered = sorted(float(x) for x in samples_ms)
    mean_val = sum(ordered) / n
    if n > 1:
        variance = sum((x - mean_val) ** 2 for x in ordered) / (n - 1)
        std_val = math.sqrt(variance)
    else:
        std_val = 0.0

    return LatencySummary(
        count=n,
        mean_ms=mean_val,
        std_ms=std_val,
        min_ms=ordered[0],
        p50_ms=compute_percentile(ordered, 50.0),
        p95_ms=compute_percentile(ordered, 95.0),
        p99_ms=compute_percentile(ordered, 99.0),
        max_ms=ordered[-1],
    )


def verify_mutual_exclusion_safety(
    records: Sequence[CriticalSectionRecord],
) -> SafetyVerificationResult:
    """Verifica que nunca dois nós estiveram simultaneamente na Seção Crítica.

    Valida tanto a ordem causal lógica de Lamport (prev.exit_lamport <
    curr.enter_lamport) quanto a disjunção dos intervalos físicos
    [enter_wall_ns, exit_wall_ns] para o mesmo recurso.
    """
    if not records:
        return SafetyVerificationResult(passed=True, total_events=0, violations=())

    violations: list[str] = []
    ordered_by_lamport = sorted(
        records,
        key=lambda r: (r.enter_lamport, r.node_id),
    )

    for idx in range(1, len(ordered_by_lamport)):
        prev = ordered_by_lamport[idx - 1]
        curr = ordered_by_lamport[idx]
        if prev.resource_tag != curr.resource_tag:
            continue
        if prev.exit_lamport >= curr.enter_lamport:
            violations.append(
                f"Sobreposição lógica de Lamport entre nó {prev.node_id} "
                f"[{prev.enter_lamport}, {prev.exit_lamport}] e nó {curr.node_id} "
                f"[{curr.enter_lamport}, {curr.exit_lamport}]."
            )

    ordered_by_wall = sorted(
        records,
        key=lambda r: (r.enter_wall_ns, r.exit_wall_ns, r.node_id),
    )
    for idx in range(1, len(ordered_by_wall)):
        prev = ordered_by_wall[idx - 1]
        curr = ordered_by_wall[idx]
        if prev.resource_tag != curr.resource_tag:
            continue
        if prev.exit_wall_ns > curr.enter_wall_ns:
            violations.append(
                f"Sobreposição física (wall-clock) entre nó {prev.node_id} "
                f"[{prev.enter_wall_ns}, {prev.exit_wall_ns}] e nó {curr.node_id} "
                f"[{curr.enter_wall_ns}, {curr.exit_wall_ns}]."
            )

    return SafetyVerificationResult(
        passed=len(violations) == 0,
        total_events=len(records),
        violations=tuple(violations),
    )


def verify_quorum_consistency(
    operations: Sequence[QuorumOperationRecord],
) -> SafetyVerificationResult:
    """Verifica a propriedade de Consistência Forte (Monotonic / Linearizable Reads).

    Garante que toda leitura bem-sucedida iniciada após o término de uma escrita
    bem-sucedida na mesma chave observa uma versão >= à versão daquela escrita.
    """
    successful_writes = [
        op for op in operations if op.op_type == "WRITE" and op.success
    ]
    successful_reads = [op for op in operations if op.op_type == "READ" and op.success]

    violations: list[str] = []
    for read_op in successful_reads:
        preceding_writes = [
            w
            for w in successful_writes
            if w.key == read_op.key and w.end_ns <= read_op.start_ns
        ]
        if not preceding_writes:
            continue
        latest_write = max(
            preceding_writes,
            key=lambda w: (w.version, w.writer_id),
        )
        if (read_op.version, read_op.writer_id) < (
            latest_write.version,
            latest_write.writer_id,
        ):
            violations.append(
                f"Leitura obsoleta (stale read) na chave '{read_op.key}': "
                f"esperado >= ({latest_write.version}, {latest_write.writer_id}), "
                f"obtido ({read_op.version}, {read_op.writer_id})."
            )

    return SafetyVerificationResult(
        passed=len(violations) == 0,
        total_events=len(successful_reads) + len(successful_writes),
        violations=tuple(violations),
    )

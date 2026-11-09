"""Injetor determinístico de falhas de nó e condições adversas de rede."""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FaultInjectionDecision:
    """Decisão tomada pelo injetor para uma mensagem enviada/recebida."""

    allowed: bool
    delay_seconds: float
    reason: str = "ok"


@dataclass(frozen=True, slots=True)
class FaultInjectorStats:
    """Estatísticas acumuladas de falhas injetadas no nó."""

    crashed: bool
    partitioned_peers: frozenset[int]
    drop_probability: float
    delay_ms: float
    jitter_ms: float
    dropped_count: int
    partition_blocked_count: int
    crash_blocked_count: int
    delayed_count: int


class FaultInjector:
    """Controlador determinístico (com seed) de falhas de processo e de canal."""

    def __init__(self, seed: int = 42) -> None:
        self._seed = seed
        self._rng = random.Random(seed)
        self._crashed: bool = False
        self._partitioned_peers: set[int] = set()
        self._drop_probability: float = 0.0
        self._delay_ms: float = 0.0
        self._jitter_ms: float = 0.0

        self._dropped_count: int = 0
        self._partition_blocked_count: int = 0
        self._crash_blocked_count: int = 0
        self._delayed_count: int = 0

    def configure(
        self,
        *,
        crashed: bool | None = None,
        partitioned_peers: Iterable[int] | None = None,
        drop_probability: float | None = None,
        delay_ms: float | None = None,
        jitter_ms: float | None = None,
        seed: int | None = None,
    ) -> None:
        """Atualiza parâmetros de falha e opcionalmente reinicia o gerador com seed."""
        if seed is not None:
            self._seed = seed
            self._rng = random.Random(seed)
        if crashed is not None:
            self._crashed = crashed
        if partitioned_peers is not None:
            self._partitioned_peers = set(partitioned_peers)
        if drop_probability is not None:
            if not (0.0 <= drop_probability <= 1.0):
                raise ValueError("drop_probability deve estar entre 0.0 e 1.0.")
            self._drop_probability = drop_probability
        if delay_ms is not None:
            if delay_ms < 0.0:
                raise ValueError("delay_ms deve ser não-negativo.")
            self._delay_ms = delay_ms
        if jitter_ms is not None:
            if jitter_ms < 0.0:
                raise ValueError("jitter_ms deve ser não-negativo.")
            self._jitter_ms = jitter_ms

    def reset(self, seed: int = 42) -> None:
        """Remove todas as falhas ativas e zera contadores."""
        self.configure(
            crashed=False,
            partitioned_peers=(),
            drop_probability=0.0,
            delay_ms=0.0,
            jitter_ms=0.0,
            seed=seed,
        )
        self._dropped_count = 0
        self._partition_blocked_count = 0
        self._crash_blocked_count = 0
        self._delayed_count = 0

    @property
    def is_crashed(self) -> bool:
        return self._crashed

    def intercept(self, peer_id: int | None = None) -> FaultInjectionDecision:
        """Avalia se uma mensagem para/de peer_id deve ser bloqueada ou atrasada."""
        if self._crashed:
            self._crash_blocked_count += 1
            return FaultInjectionDecision(
                allowed=False,
                delay_seconds=0.0,
                reason="node_crashed",
            )
        if peer_id is not None and peer_id in self._partitioned_peers:
            self._partition_blocked_count += 1
            return FaultInjectionDecision(
                allowed=False,
                delay_seconds=0.0,
                reason="network_partition",
            )
        if self._drop_probability > 0.0 and self._rng.random() < self._drop_probability:
            self._dropped_count += 1
            return FaultInjectionDecision(
                allowed=False,
                delay_seconds=0.0,
                reason="packet_dropped",
            )
        delay_sec = self._sample_delay_seconds()
        if delay_sec > 0.0:
            self._delayed_count += 1
        return FaultInjectionDecision(
            allowed=True,
            delay_seconds=delay_sec,
            reason="ok",
        )

    def stats(self) -> FaultInjectorStats:
        """Retorna fotografia das configurações e contadores de falha."""
        return FaultInjectorStats(
            crashed=self._crashed,
            partitioned_peers=frozenset(self._partitioned_peers),
            drop_probability=self._drop_probability,
            delay_ms=self._delay_ms,
            jitter_ms=self._jitter_ms,
            dropped_count=self._dropped_count,
            partition_blocked_count=self._partition_blocked_count,
            crash_blocked_count=self._crash_blocked_count,
            delayed_count=self._delayed_count,
        )

    def _sample_delay_seconds(self) -> float:
        if self._delay_ms <= 0.0 and self._jitter_ms <= 0.0:
            return 0.0
        jitter = (
            self._rng.uniform(-self._jitter_ms, self._jitter_ms)
            if self._jitter_ms > 0.0
            else 0.0
        )
        total_ms = max(0.0, self._delay_ms + jitter)
        return total_ms / 1000.0

"""Relógio Lógico de Lamport e timestamps totalmente ordenados."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True, slots=True)
class LogicalTimestamp:
    """Par (timestamp, node_id) que define uma ordem total estrita de Lamport."""

    timestamp: int
    node_id: int


class LamportClock:
    """Implementação determinística do Relógio Lógico de Lamport."""

    def __init__(self, node_id: int, initial_time: int = 0) -> None:
        if node_id < 0:
            raise ValueError("node_id deve ser não-negativo.")
        if initial_time < 0:
            raise ValueError("initial_time deve ser não-negativo.")
        self._node_id = node_id
        self._time = initial_time

    @property
    def node_id(self) -> int:
        return self._node_id

    @property
    def time(self) -> int:
        return self._time

    def tick(self) -> int:
        """Incrementa o relógio para um evento local ou envio de mensagem."""
        self._time += 1
        return self._time

    def update(self, received_time: int) -> int:
        """Atualiza o relógio na recepção de mensagem: L_i = max(L_i, L_msg) + 1."""
        if received_time < 0:
            raise ValueError("received_time deve ser não-negativo.")
        self._time = max(self._time, received_time) + 1
        return self._time

    def snapshot(self) -> LogicalTimestamp:
        """Retorna o par (L_i, i) atual sem avançar o relógio."""
        return LogicalTimestamp(timestamp=self._time, node_id=self._node_id)

"""Replicação e Consistência Baseada em Quorum (Protocolo de Gifford + Read-Repair)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class QuorumConfig:
    """Parâmetros de quórum de Gifford (N, N_R, N_W) com validação das invariantes."""

    total_nodes: int
    read_quorum: int
    write_quorum: int

    def __post_init__(self) -> None:
        if self.total_nodes < 3:
            raise ValueError("O sistema deve possuir no mínimo 3 nós lógicos (N >= 3).")
        if not (1 <= self.read_quorum <= self.total_nodes):
            raise ValueError("read_quorum (N_R) deve estar entre 1 e N.")
        if not (1 <= self.write_quorum <= self.total_nodes):
            raise ValueError("write_quorum (N_W) deve estar entre 1 e N.")
        if self.read_quorum + self.write_quorum <= self.total_nodes:
            raise ValueError(
                f"Violação de interseção leitura-escrita: N_R ({self.read_quorum}) + "
                f"N_W ({self.write_quorum}) deve ser > N ({self.total_nodes})."
            )
        if self.write_quorum * 2 <= self.total_nodes:
            raise ValueError(
                f"Violação de interseção escrita-escrita: N_W ({self.write_quorum}) "
                f"deve ser > N/2 ({self.total_nodes / 2})."
            )


@dataclass(frozen=True, slots=True)
class VersionedValue:
    """Valor versionado com ordenação determinística por (version, writer_id)."""

    value: str
    version: int
    writer_id: int
    lamport_ts: int
    updated_wall_ns: int = 0

    @property
    def version_key(self) -> tuple[int, int]:
        """Tupla de ordenação total de versão (version, writer_id)."""
        return (self.version, self.writer_id)

    @property
    def found(self) -> bool:
        """Retorna True se a chave já foi escrita ao menos uma vez (version > 0)."""
        return self.version > 0


EMPTY_VALUE = VersionedValue(
    value="",
    version=0,
    writer_id=0,
    lamport_ts=0,
    updated_wall_ns=0,
)


@dataclass(frozen=True, slots=True)
class QuorumReadResult:
    """Resultado consolidado de uma leitura por quórum."""

    key: str
    value: VersionedValue
    acks_received: int
    quorum_required: int
    stale_nodes_repaired: frozenset[int]
    latency_ms: float


@dataclass(frozen=True, slots=True)
class QuorumWriteResult:
    """Resultado consolidado de uma escrita por quórum em duas fases."""

    key: str
    value: VersionedValue
    read_phase_acks: int
    write_phase_acks: int
    quorum_required: int
    latency_ms: float


class LocalReplicaStore:
    """Armazenamento local chave-valor de uma réplica com atualização monotônica."""

    def __init__(self) -> None:
        self._data: dict[str, VersionedValue] = {}

    def get(self, key: str) -> VersionedValue:
        """Lê a versão local da chave (ou EMPTY_VALUE se inexistente)."""
        return self._data.get(key, EMPTY_VALUE)

    def put_if_newer(
        self,
        key: str,
        candidate: VersionedValue,
    ) -> tuple[bool, VersionedValue]:
        """Aplica candidate somente se (version, writer_id) for estritamente maior."""
        current = self._data.get(key, EMPTY_VALUE)
        if candidate.version_key > current.version_key:
            self._data[key] = candidate
            return True, candidate
        return False, current

    def snapshot(self) -> dict[str, VersionedValue]:
        """Retorna cópia imutável do estado atual da réplica."""
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)


def select_highest_version(
    responses: Mapping[int, VersionedValue],
) -> VersionedValue:
    """Seleciona o valor com maior (version, writer_id) dentre as réplicas."""
    if not responses:
        return EMPTY_VALUE
    return max(responses.values(), key=lambda item: item.version_key)


def compute_next_version(
    responses: Mapping[int, VersionedValue],
    writer_id: int,
    new_value: str,
    lamport_ts: int,
    now_wall_ns: int,
) -> VersionedValue:
    """Calcula a próxima versão monotônica v_new = max(v) + 1 a partir do quórum."""
    highest = select_highest_version(responses)
    return VersionedValue(
        value=new_value,
        version=highest.version + 1,
        writer_id=writer_id,
        lamport_ts=lamport_ts,
        updated_wall_ns=now_wall_ns,
    )


def find_stale_replicas(
    responses: Mapping[int, VersionedValue],
    latest: VersionedValue,
) -> frozenset[int]:
    """Identifica os IDs das réplicas que retornaram versão inferior à mais recente."""
    if not latest.found:
        return frozenset()
    return frozenset(
        node_id
        for node_id, val in responses.items()
        if val.version_key < latest.version_key
    )

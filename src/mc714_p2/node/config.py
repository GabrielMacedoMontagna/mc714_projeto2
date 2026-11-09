"""Configuração parametrizada por variáveis de ambiente e linha de comando."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from mc714_p2.domain.quorum import QuorumConfig
from mc714_p2.rpc.client import PeerEndpoint


def parse_peers_spec(spec: str) -> dict[int, PeerEndpoint]:
    """Interpreta string '1=host1:50051,2=host2:50052' em mapa de PeerEndpoint."""
    peers: dict[int, PeerEndpoint] = {}
    cleaned = spec.strip()
    if not cleaned:
        return peers
    for entry in cleaned.split(","):
        token = entry.strip()
        if not token:
            continue
        if "=" not in token or ":" not in token:
            raise ValueError(
                f"Formato inválido de peer '{token}'. Esperado 'id=host:port'."
            )
        id_part, addr_part = token.split("=", maxsplit=1)
        host_part, port_part = addr_part.rsplit(":", maxsplit=1)
        node_id = int(id_part.strip())
        port = int(port_part.strip())
        peers[node_id] = PeerEndpoint(
            node_id=node_id,
            host=host_part.strip(),
            port=port,
        )
    return peers


class NodeSettings(BaseSettings):
    """Configurações de execução de um nó distribuído (12-Factor App)."""

    model_config = SettingsConfigDict(
        env_prefix="MC714_",
        case_sensitive=False,
        extra="ignore",
    )

    node_id: int = Field(default=1, ge=1)
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=50051, ge=1, le=65535)
    peers: str = Field(default="1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053")
    quorum_r: int = Field(default=2, ge=1)
    quorum_w: int = Field(default=2, ge=1)
    hb_interval_ms: float = Field(default=80.0, gt=0.0)
    hb_timeout_ms: float = Field(default=280.0, gt=0.0)
    rpc_timeout_ms: float = Field(default=400.0, gt=0.0)
    fault_seed: int = Field(default=42)

    def peer_endpoints(self) -> dict[int, PeerEndpoint]:
        """Retorna todos os endpoints configurados no cluster."""
        return parse_peers_spec(self.peers)

    def remote_peer_endpoints(self) -> dict[int, PeerEndpoint]:
        """Retorna apenas os endpoints dos outros nós (excluindo o próprio node_id)."""
        return {
            pid: ep for pid, ep in self.peer_endpoints().items() if pid != self.node_id
        }

    def quorum_config(self) -> QuorumConfig:
        """Constrói e valida a configuração de quórum (N, N_R, N_W)."""
        all_peers = self.peer_endpoints()
        total = max(len(all_peers), 3)
        return QuorumConfig(
            total_nodes=total,
            read_quorum=self.quorum_r,
            write_quorum=self.quorum_w,
        )

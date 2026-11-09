"""Testes unitários de configuração de nós e obrigatoriedade de timeouts em gRPC."""

from __future__ import annotations

import pytest

from mc714_p2.node.config import NodeSettings, parse_peers_spec
from mc714_p2.rpc.client import PeerEndpoint, PeerRpcClient


def test_parse_peers_spec_and_node_settings() -> None:
    spec = "1=node-1:50051, 2=node-2:50052, 3=node-3:50053"
    parsed = parse_peers_spec(spec)
    assert len(parsed) == 3
    assert parsed[2] == PeerEndpoint(node_id=2, host="node-2", port=50052)
    assert parsed[2].target == "node-2:50052"

    settings = NodeSettings(
        node_id=1,
        peers=spec,
        quorum_r=2,
        quorum_w=2,
    )
    assert set(settings.remote_peer_endpoints().keys()) == {2, 3}
    qcfg = settings.quorum_config()
    assert qcfg.total_nodes == 3


@pytest.mark.asyncio
async def test_grpc_client_rejects_missing_or_non_positive_timeout() -> None:
    client = PeerRpcClient({2: PeerEndpoint(node_id=2, host="127.0.0.1", port=59999)})
    with pytest.raises(ValueError, match="Timeouts explícitos"):
        await client.send_heartbeat(
            peer_id=2,
            sender_id=1,
            sequence=1,
            lamport_ts=1,
            timeout_sec=0.0,
        )
    with pytest.raises(ValueError, match="Timeouts explícitos"):
        await client.replica_read(
            peer_id=2,
            coordinator_id=1,
            key="k",
            lamport_ts=1,
            timeout_sec=-0.5,
        )
    await client.close()

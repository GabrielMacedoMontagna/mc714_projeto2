from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HeartbeatMsg(_message.Message):
    __slots__ = ("sender_id", "sequence", "lamport_ts")
    SENDER_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    sender_id: int
    sequence: int
    lamport_ts: int
    def __init__(self, sender_id: _Optional[int] = ..., sequence: _Optional[int] = ..., lamport_ts: _Optional[int] = ...) -> None: ...

class HeartbeatAck(_message.Message):
    __slots__ = ("responder_id", "sequence", "lamport_ts", "accepted")
    RESPONDER_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    responder_id: int
    sequence: int
    lamport_ts: int
    accepted: bool
    def __init__(self, responder_id: _Optional[int] = ..., sequence: _Optional[int] = ..., lamport_ts: _Optional[int] = ..., accepted: _Optional[bool] = ...) -> None: ...

class MutexRequestMsg(_message.Message):
    __slots__ = ("requester_id", "request_lamport_ts", "resource_tag")
    REQUESTER_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TAG_FIELD_NUMBER: _ClassVar[int]
    requester_id: int
    request_lamport_ts: int
    resource_tag: str
    def __init__(self, requester_id: _Optional[int] = ..., request_lamport_ts: _Optional[int] = ..., resource_tag: _Optional[str] = ...) -> None: ...

class MutexRequestAck(_message.Message):
    __slots__ = ("responder_id", "granted_immediately", "responder_lamport_ts")
    RESPONDER_ID_FIELD_NUMBER: _ClassVar[int]
    GRANTED_IMMEDIATELY_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    responder_id: int
    granted_immediately: bool
    responder_lamport_ts: int
    def __init__(self, responder_id: _Optional[int] = ..., granted_immediately: _Optional[bool] = ..., responder_lamport_ts: _Optional[int] = ...) -> None: ...

class MutexReplyMsg(_message.Message):
    __slots__ = ("responder_id", "responder_lamport_ts")
    RESPONDER_ID_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    responder_id: int
    responder_lamport_ts: int
    def __init__(self, responder_id: _Optional[int] = ..., responder_lamport_ts: _Optional[int] = ...) -> None: ...

class MutexAck(_message.Message):
    __slots__ = ("ok",)
    OK_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    def __init__(self, ok: _Optional[bool] = ...) -> None: ...

class ReplicaReadMsg(_message.Message):
    __slots__ = ("coordinator_id", "key", "lamport_ts")
    COORDINATOR_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    coordinator_id: int
    key: str
    lamport_ts: int
    def __init__(self, coordinator_id: _Optional[int] = ..., key: _Optional[str] = ..., lamport_ts: _Optional[int] = ...) -> None: ...

class ReplicaReadAck(_message.Message):
    __slots__ = ("replica_id", "key", "value", "version", "writer_id", "value_lamport_ts", "responder_lamport_ts", "found")
    REPLICA_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    replica_id: int
    key: str
    value: str
    version: int
    writer_id: int
    value_lamport_ts: int
    responder_lamport_ts: int
    found: bool
    def __init__(self, replica_id: _Optional[int] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., version: _Optional[int] = ..., writer_id: _Optional[int] = ..., value_lamport_ts: _Optional[int] = ..., responder_lamport_ts: _Optional[int] = ..., found: _Optional[bool] = ...) -> None: ...

class ReplicaWriteMsg(_message.Message):
    __slots__ = ("coordinator_id", "key", "value", "version", "writer_id", "value_lamport_ts", "sender_lamport_ts", "is_read_repair")
    COORDINATOR_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    SENDER_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    IS_READ_REPAIR_FIELD_NUMBER: _ClassVar[int]
    coordinator_id: int
    key: str
    value: str
    version: int
    writer_id: int
    value_lamport_ts: int
    sender_lamport_ts: int
    is_read_repair: bool
    def __init__(self, coordinator_id: _Optional[int] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., version: _Optional[int] = ..., writer_id: _Optional[int] = ..., value_lamport_ts: _Optional[int] = ..., sender_lamport_ts: _Optional[int] = ..., is_read_repair: _Optional[bool] = ...) -> None: ...

class ReplicaWriteAck(_message.Message):
    __slots__ = ("replica_id", "applied", "current_version", "current_writer_id", "responder_lamport_ts")
    REPLICA_ID_FIELD_NUMBER: _ClassVar[int]
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    CURRENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    replica_id: int
    applied: bool
    current_version: int
    current_writer_id: int
    responder_lamport_ts: int
    def __init__(self, replica_id: _Optional[int] = ..., applied: _Optional[bool] = ..., current_version: _Optional[int] = ..., current_writer_id: _Optional[int] = ..., responder_lamport_ts: _Optional[int] = ...) -> None: ...

class ClientCSRequest(_message.Message):
    __slots__ = ("resource_tag", "hold_ms", "timeout_ms", "optional_quorum_key", "optional_quorum_value")
    RESOURCE_TAG_FIELD_NUMBER: _ClassVar[int]
    HOLD_MS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    OPTIONAL_QUORUM_KEY_FIELD_NUMBER: _ClassVar[int]
    OPTIONAL_QUORUM_VALUE_FIELD_NUMBER: _ClassVar[int]
    resource_tag: str
    hold_ms: float
    timeout_ms: float
    optional_quorum_key: str
    optional_quorum_value: str
    def __init__(self, resource_tag: _Optional[str] = ..., hold_ms: _Optional[float] = ..., timeout_ms: _Optional[float] = ..., optional_quorum_key: _Optional[str] = ..., optional_quorum_value: _Optional[str] = ...) -> None: ...

class ClientCSReply(_message.Message):
    __slots__ = ("acquired", "node_id", "request_lamport_ts", "enter_lamport_ts", "exit_lamport_ts", "enter_wall_ns", "exit_wall_ns", "wait_latency_ms", "protocol_messages_used", "error_message")
    ACQUIRED_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    ENTER_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    EXIT_LAMPORT_TS_FIELD_NUMBER: _ClassVar[int]
    ENTER_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    EXIT_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    WAIT_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_MESSAGES_USED_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    acquired: bool
    node_id: int
    request_lamport_ts: int
    enter_lamport_ts: int
    exit_lamport_ts: int
    enter_wall_ns: int
    exit_wall_ns: int
    wait_latency_ms: float
    protocol_messages_used: int
    error_message: str
    def __init__(self, acquired: _Optional[bool] = ..., node_id: _Optional[int] = ..., request_lamport_ts: _Optional[int] = ..., enter_lamport_ts: _Optional[int] = ..., exit_lamport_ts: _Optional[int] = ..., enter_wall_ns: _Optional[int] = ..., exit_wall_ns: _Optional[int] = ..., wait_latency_ms: _Optional[float] = ..., protocol_messages_used: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class ClientReadRequest(_message.Message):
    __slots__ = ("key", "enable_read_repair", "override_read_quorum")
    KEY_FIELD_NUMBER: _ClassVar[int]
    ENABLE_READ_REPAIR_FIELD_NUMBER: _ClassVar[int]
    OVERRIDE_READ_QUORUM_FIELD_NUMBER: _ClassVar[int]
    key: str
    enable_read_repair: bool
    override_read_quorum: int
    def __init__(self, key: _Optional[str] = ..., enable_read_repair: _Optional[bool] = ..., override_read_quorum: _Optional[int] = ...) -> None: ...

class ClientReadReply(_message.Message):
    __slots__ = ("success", "found", "key", "value", "version", "writer_id", "acks_received", "quorum_required", "repaired_nodes", "latency_ms", "start_wall_ns", "end_wall_ns", "error_message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    ACKS_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    QUORUM_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    REPAIRED_NODES_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    START_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    END_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    found: bool
    key: str
    value: str
    version: int
    writer_id: int
    acks_received: int
    quorum_required: int
    repaired_nodes: _containers.RepeatedScalarFieldContainer[int]
    latency_ms: float
    start_wall_ns: int
    end_wall_ns: int
    error_message: str
    def __init__(self, success: _Optional[bool] = ..., found: _Optional[bool] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., version: _Optional[int] = ..., writer_id: _Optional[int] = ..., acks_received: _Optional[int] = ..., quorum_required: _Optional[int] = ..., repaired_nodes: _Optional[_Iterable[int]] = ..., latency_ms: _Optional[float] = ..., start_wall_ns: _Optional[int] = ..., end_wall_ns: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class ClientWriteRequest(_message.Message):
    __slots__ = ("key", "value", "override_write_quorum")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    OVERRIDE_WRITE_QUORUM_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    override_write_quorum: int
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ..., override_write_quorum: _Optional[int] = ...) -> None: ...

class ClientWriteReply(_message.Message):
    __slots__ = ("success", "key", "value", "version", "writer_id", "read_phase_acks", "write_phase_acks", "quorum_required", "latency_ms", "start_wall_ns", "end_wall_ns", "error_message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_ID_FIELD_NUMBER: _ClassVar[int]
    READ_PHASE_ACKS_FIELD_NUMBER: _ClassVar[int]
    WRITE_PHASE_ACKS_FIELD_NUMBER: _ClassVar[int]
    QUORUM_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    START_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    END_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    key: str
    value: str
    version: int
    writer_id: int
    read_phase_acks: int
    write_phase_acks: int
    quorum_required: int
    latency_ms: float
    start_wall_ns: int
    end_wall_ns: int
    error_message: str
    def __init__(self, success: _Optional[bool] = ..., key: _Optional[str] = ..., value: _Optional[str] = ..., version: _Optional[int] = ..., writer_id: _Optional[int] = ..., read_phase_acks: _Optional[int] = ..., write_phase_acks: _Optional[int] = ..., quorum_required: _Optional[int] = ..., latency_ms: _Optional[float] = ..., start_wall_ns: _Optional[int] = ..., end_wall_ns: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...

class FaultConfigMsg(_message.Message):
    __slots__ = ("reset_all", "set_crashed", "crashed", "set_partition", "partitioned_peers", "set_drop", "drop_probability", "set_delay", "delay_ms", "jitter_ms", "set_seed", "seed", "set_hb_timeout", "hb_timeout_ms")
    RESET_ALL_FIELD_NUMBER: _ClassVar[int]
    SET_CRASHED_FIELD_NUMBER: _ClassVar[int]
    CRASHED_FIELD_NUMBER: _ClassVar[int]
    SET_PARTITION_FIELD_NUMBER: _ClassVar[int]
    PARTITIONED_PEERS_FIELD_NUMBER: _ClassVar[int]
    SET_DROP_FIELD_NUMBER: _ClassVar[int]
    DROP_PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    SET_DELAY_FIELD_NUMBER: _ClassVar[int]
    DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    JITTER_MS_FIELD_NUMBER: _ClassVar[int]
    SET_SEED_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SET_HB_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    HB_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    reset_all: bool
    set_crashed: bool
    crashed: bool
    set_partition: bool
    partitioned_peers: _containers.RepeatedScalarFieldContainer[int]
    set_drop: bool
    drop_probability: float
    set_delay: bool
    delay_ms: float
    jitter_ms: float
    set_seed: bool
    seed: int
    set_hb_timeout: bool
    hb_timeout_ms: float
    def __init__(self, reset_all: _Optional[bool] = ..., set_crashed: _Optional[bool] = ..., crashed: _Optional[bool] = ..., set_partition: _Optional[bool] = ..., partitioned_peers: _Optional[_Iterable[int]] = ..., set_drop: _Optional[bool] = ..., drop_probability: _Optional[float] = ..., set_delay: _Optional[bool] = ..., delay_ms: _Optional[float] = ..., jitter_ms: _Optional[float] = ..., set_seed: _Optional[bool] = ..., seed: _Optional[int] = ..., set_hb_timeout: _Optional[bool] = ..., hb_timeout_ms: _Optional[float] = ...) -> None: ...

class FaultConfigAck(_message.Message):
    __slots__ = ("ok", "crashed", "partitioned_peers", "drop_probability", "delay_ms", "jitter_ms")
    OK_FIELD_NUMBER: _ClassVar[int]
    CRASHED_FIELD_NUMBER: _ClassVar[int]
    PARTITIONED_PEERS_FIELD_NUMBER: _ClassVar[int]
    DROP_PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    JITTER_MS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    crashed: bool
    partitioned_peers: _containers.RepeatedScalarFieldContainer[int]
    drop_probability: float
    delay_ms: float
    jitter_ms: float
    def __init__(self, ok: _Optional[bool] = ..., crashed: _Optional[bool] = ..., partitioned_peers: _Optional[_Iterable[int]] = ..., drop_probability: _Optional[float] = ..., delay_ms: _Optional[float] = ..., jitter_ms: _Optional[float] = ...) -> None: ...

class StatusRequestMsg(_message.Message):
    __slots__ = ("include_history",)
    INCLUDE_HISTORY_FIELD_NUMBER: _ClassVar[int]
    include_history: bool
    def __init__(self, include_history: _Optional[bool] = ...) -> None: ...

class CSRecordMsg(_message.Message):
    __slots__ = ("node_id", "request_lamport", "enter_lamport", "exit_lamport", "enter_wall_ns", "exit_wall_ns", "resource_tag")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_LAMPORT_FIELD_NUMBER: _ClassVar[int]
    ENTER_LAMPORT_FIELD_NUMBER: _ClassVar[int]
    EXIT_LAMPORT_FIELD_NUMBER: _ClassVar[int]
    ENTER_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    EXIT_WALL_NS_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TAG_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    request_lamport: int
    enter_lamport: int
    exit_lamport: int
    enter_wall_ns: int
    exit_wall_ns: int
    resource_tag: str
    def __init__(self, node_id: _Optional[int] = ..., request_lamport: _Optional[int] = ..., enter_lamport: _Optional[int] = ..., exit_lamport: _Optional[int] = ..., enter_wall_ns: _Optional[int] = ..., exit_wall_ns: _Optional[int] = ..., resource_tag: _Optional[str] = ...) -> None: ...

class PeerHealthMsg(_message.Message):
    __slots__ = ("peer_id", "status", "last_sequence", "suspect_count")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LAST_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    SUSPECT_COUNT_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    status: str
    last_sequence: int
    suspect_count: int
    def __init__(self, peer_id: _Optional[int] = ..., status: _Optional[str] = ..., last_sequence: _Optional[int] = ..., suspect_count: _Optional[int] = ...) -> None: ...

class StatusReplyMsg(_message.Message):
    __slots__ = ("node_id", "lamport_clock", "mutex_state", "active_peers", "suspected_peers", "peer_health", "replica_keys_count", "is_crashed", "mutex_messages_sent", "mutex_messages_received", "quorum_messages_sent", "quorum_messages_received", "heartbeat_messages_sent", "heartbeat_messages_received", "dropped_messages", "partition_blocked_messages", "cs_history")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    LAMPORT_CLOCK_FIELD_NUMBER: _ClassVar[int]
    MUTEX_STATE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_PEERS_FIELD_NUMBER: _ClassVar[int]
    SUSPECTED_PEERS_FIELD_NUMBER: _ClassVar[int]
    PEER_HEALTH_FIELD_NUMBER: _ClassVar[int]
    REPLICA_KEYS_COUNT_FIELD_NUMBER: _ClassVar[int]
    IS_CRASHED_FIELD_NUMBER: _ClassVar[int]
    MUTEX_MESSAGES_SENT_FIELD_NUMBER: _ClassVar[int]
    MUTEX_MESSAGES_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    QUORUM_MESSAGES_SENT_FIELD_NUMBER: _ClassVar[int]
    QUORUM_MESSAGES_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_MESSAGES_SENT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_MESSAGES_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    DROPPED_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BLOCKED_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    CS_HISTORY_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    lamport_clock: int
    mutex_state: str
    active_peers: _containers.RepeatedScalarFieldContainer[int]
    suspected_peers: _containers.RepeatedScalarFieldContainer[int]
    peer_health: _containers.RepeatedCompositeFieldContainer[PeerHealthMsg]
    replica_keys_count: int
    is_crashed: bool
    mutex_messages_sent: int
    mutex_messages_received: int
    quorum_messages_sent: int
    quorum_messages_received: int
    heartbeat_messages_sent: int
    heartbeat_messages_received: int
    dropped_messages: int
    partition_blocked_messages: int
    cs_history: _containers.RepeatedCompositeFieldContainer[CSRecordMsg]
    def __init__(self, node_id: _Optional[int] = ..., lamport_clock: _Optional[int] = ..., mutex_state: _Optional[str] = ..., active_peers: _Optional[_Iterable[int]] = ..., suspected_peers: _Optional[_Iterable[int]] = ..., peer_health: _Optional[_Iterable[_Union[PeerHealthMsg, _Mapping]]] = ..., replica_keys_count: _Optional[int] = ..., is_crashed: _Optional[bool] = ..., mutex_messages_sent: _Optional[int] = ..., mutex_messages_received: _Optional[int] = ..., quorum_messages_sent: _Optional[int] = ..., quorum_messages_received: _Optional[int] = ..., heartbeat_messages_sent: _Optional[int] = ..., heartbeat_messages_received: _Optional[int] = ..., dropped_messages: _Optional[int] = ..., partition_blocked_messages: _Optional[int] = ..., cs_history: _Optional[_Iterable[_Union[CSRecordMsg, _Mapping]]] = ...) -> None: ...

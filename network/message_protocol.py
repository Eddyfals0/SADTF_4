"""
Message protocol for P2P communication.
Defines message serialization and deserialization.
"""
import json
import time
from typing import Any, Dict, Optional
from enum import Enum


class MessageType(Enum):
    """Types of messages in the P2P protocol."""
    CONNECT = "CONNECT"
    CONNECT_ACK = "CONNECT_ACK"
    RECONNECT = "RECONNECT"
    RECONNECT_ACK = "RECONNECT_ACK"
    NODE_DISCOVERY = "NODE_DISCOVERY"
    FILE_UPLOAD = "FILE_UPLOAD"
    FILE_DOWNLOAD = "FILE_DOWNLOAD"
    BLOCK_REQUEST = "BLOCK_REQUEST"
    BLOCK_SEND = "BLOCK_SEND"
    METADATA_SYNC = "METADATA_SYNC"
    DELETE_FILE = "DELETE_FILE"
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"


class Message:
    """Represents a message in the P2P protocol."""
    
    def __init__(self, msg_type: MessageType, sender_id: int, payload: Dict[str, Any] = None):
        self.type = msg_type
        self.sender_id = sender_id
        self.payload = payload or {}
        self.timestamp = time.time()
    
    def serialize(self) -> bytes:
        """Serialize message to JSON bytes."""
        data = {
            "type": self.type.value,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp
        }
        json_str = json.dumps(data)
        return json_str.encode('utf-8')
    
    @staticmethod
    def deserialize(data: bytes) -> 'Message':
        """Deserialize message from JSON bytes."""
        try:
            json_str = data.decode('utf-8')
            data_dict = json.loads(json_str)
            msg_type = MessageType(data_dict["type"])
            sender_id = data_dict["sender_id"]
            payload = data_dict.get("payload", {})
            msg = Message(msg_type, sender_id, payload)
            msg.timestamp = data_dict.get("timestamp", time.time())
            return msg
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(f"Failed to deserialize message: {e}")
    
    def __repr__(self):
        return f"Message(type={self.type.value}, sender_id={self.sender_id}, payload={self.payload})"


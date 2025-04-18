# protocol.py
from __future__ import annotations
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Type, get_type_hints

import jsonschema
from jsonschema import Draft7Validator, ValidationError

# Custom Exceptions
class SchemaValidationError(ValidationError):
    """Extended validation error with protocol context"""
    def __init__(self, message: str, protocol: str, path: list):
        super().__init__(message)
        self.protocol = protocol
        self.path = path

# Base Protocol Class
@dataclass
class MessageProtocol:
    """Base class for all message schemas"""
    @classmethod
    def schema(cls) -> Dict[str, Any]:
        return generate_schema(cls)

# Protocol Implementations
@dataclass
class HeartbeatProtocol(MessageProtocol):
    agent_id: str
    timestamp: datetime
    status: str  # "active", "idle", "error"
    metrics: Dict[str, float]

@dataclass
class TaskProtocol(MessageProtocol):
    task_id: str
    payload: Dict[str, Any]
    priority: int  # 1-5
    dependencies: list[str] = None

@dataclass 
class BroadcastProtocol(MessageProtocol):
    message_type: str  # "alert", "update", "command"
    scope: str  # "system", "cluster", "agent_group"
    content: Dict[str, Any]

# Schema Generation Logic
def generate_schema(cls: Type[MessageProtocol]) -> Dict[str, Any]:
    """Generate JSON Schema from dataclass annotations"""
    type_mapping = {
        str: {"type": "string"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bool: {"type": "boolean"},
        datetime: {"type": "string", "format": "date-time"},
        Dict[str, Any]: {"type": "object"},
    }

    schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False
    }

    for field, field_type in get_type_hints(cls).items():
        if field == "dependencies":
            schema["properties"][field] = {
                "type": "array",
                "items": {"type": "string"},
                "default": []
            }
        elif field_type in type_mapping:
            schema["properties"][field] = type_mapping[field_type]
        elif getattr(field_type, "__origin__", None) is dict:
            key_type, value_type = field_type.__args__
            schema["properties"][field] = {
                "type": "object",
                "additionalProperties": type_mapping[value_type]
            }
        else:
            schema["properties"][field] = {"$ref": f"#/definitions/{field_type.__name__}"}

        if field not in cls.__dataclass_fields__ or not cls.__dataclass_fields__[field].default:
            schema["required"].append(field)

    return schema

# Validation Engine
class ProtocolValidator:
    def __init__(self):
        self._schemas = {
            "heartbeat": HeartbeatProtocol.schema(),
            "task": TaskProtocol.schema(),
            "broadcast": BroadcastProtocol.schema()
        }

    @lru_cache(maxsize=128)
    def get_validator(self, protocol_type: str) -> Draft7Validator:
        """Cached validator instance for protocol type"""
        schema = self._schemas.get(protocol_type)
        if not schema:
            raise ValueError(f"Unsupported protocol: {protocol_type}")
        return Draft7Validator(schema)

    def validate(self, message: Dict[str, Any], protocol_type: str) -> None:
        """Validate message against specified protocol"""
        validator = self.get_validator(protocol_type)
        try:
            validator.validate(message)
        except ValidationError as ve:
            raise SchemaValidationError(
                message=f"Protocol violation in {protocol_type}: {ve.message}",
                protocol=protocol_type,
                path=list(ve.path)
            ) from ve

# Example Usage
if __name__ == "__main__":
    validator = ProtocolValidator()

    # Valid message
    valid_heartbeat = {
        "agent_id": "agent-123",
        "timestamp": datetime.utcnow().isoformat(),
        "status": "active",
        "metrics": {"cpu": 0.45, "memory": 0.7}
    }
    validator.validate(valid_heartbeat, "heartbeat")

    # Invalid message
    try:
        invalid_task = {
            "task_id": "task-456",
            "priority": "high"  # Should be integer
        }
        validator.validate(invalid_task, "task")
    except SchemaValidationError as e:
        logging.error(f"Validation failed: {e}")

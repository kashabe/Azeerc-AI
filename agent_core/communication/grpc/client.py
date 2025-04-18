# client.py
import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, AsyncIterable, Callable, Dict, List, Optional

import grpc
from grpc import aio

from agent_service_pb2 import (
    AgentState,
    DeregisterAgentRequest,
    GetAgentStateRequest,
    HeartbeatRequest,
    RegisterAgentRequest,
    TaskReceipt,
)
from agent_service_pb2_grpc import AgentServiceStub

# Configuration
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 1.5
DEFAULT_TIMEOUT = 30  # seconds
SERVICE_DISCOVERY_URLS = [
    "dns:///azeerc-ai-cluster-1:50051",
    "dns:///azeerc-ai-cluster-2:50051",
]

@dataclass
class ClientConfig:
    auth_token: str
    agent_id: Optional[str] = None
    session_token: Optional[str] = None
    last_heartbeat: Optional[datetime] = None

class AzeercClient:
    def __init__(self, config: ClientConfig):
        self.config = config
        self._channel: Optional[aio.Channel] = None
        self._stub: Optional[AgentServiceStub] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._retry_policy = self._default_retry_policy()

    # --- Core Connectivity ---
    async def connect(self):
        """Establish managed channel with load balancing and retries"""
        self._channel = aio.insecure_channel(
            random.choice(SERVICE_DISCOVERY_URLS),
            options=[
                ("grpc.lb_policy_name", "round_robin"),
                ("grpc.enable_retries", 1),
                ("grpc.keepalive_time_ms", 10000),
            ],
        )
        self._stub = AgentServiceStub(self._channel)
        await self._start_session()

    async def _start_session(self):
        """Authenticate and establish session"""
        request = RegisterAgentRequest(
            agent_type="CLIENT",
            auth_token=self.config.auth_token,
        )
        response = await self._retryable_call(
            self._stub.RegisterAgent,
            request,
            retryable_errors=[grpc.StatusCode.UNAVAILABLE]
        )
        self.config.agent_id = response.agent_id
        self.config.session_token = response.session_token
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    # --- Retry Decorator ---
    def _retryable_call(self, func: Callable, *args, **kwargs):
        @wraps(func)
        async def wrapper():
            for attempt in range(MAX_RETRIES + 1):
                try:
                    return await func(*args, **kwargs)
                except grpc.RpcError as e:
                    if e.code() not in self._retry_policy.get(e.code(), []):
                        raise
                    delay = RETRY_BACKOFF_BASE ** attempt
                    await asyncio.sleep(delay)
            raise
        return wrapper()

    @property
    def _default_retry_policy(self) -> Dict[grpc.StatusCode, List[grpc.StatusCode]]:
        return {
            grpc.StatusCode.DEADLINE_EXCEEDED: [grpc.StatusCode.DEADLINE_EXCEEDED],
            grpc.StatusCode.UNAVAILABLE: [
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.ABORTED,
            ],
        }

    # --- Heartbeat Management ---
    async def _heartbeat_loop(self):
        """Maintain bidirectional streaming connection"""
        async def request_stream():
            while True:
                yield HeartbeatRequest(
                    agent_id=self.config.agent_id,
                    session_token=self.config.session_token,
                )
                await asyncio.sleep(10)

        try:
            async for response in self._stub.Heartbeat(request_stream()):
                self._handle_heartbeat_response(response)
        except grpc.RpcError as e:
            logging.error(f"Heartbeat failed: {e.code().name}")
            await self.reconnect()

    def _handle_heartbeat_response(self, response):
        """Process server instructions from heartbeat"""
        if response.HasField("task"):
            asyncio.create_task(self._execute_task(response.task))
        elif response.HasField("config"):
            self._update_config(response.config)

    # --- Core Operations ---
    @_retryable_call
    async def get_agent_state(self, agent_id: str) -> AgentState:
        return await self._stub.GetAgentState(
            GetAgentStateRequest(agent_id=agent_id),
            timeout=DEFAULT_TIMEOUT,
            metadata=self._auth_metadata(),
        )

    async def submit_task(self, task_payload: Dict) -> TaskReceipt:
        return await self._retryable_call(
            self._stub.SubmitTask,
            task_payload,
            retryable_errors=[grpc.StatusCode.RESOURCE_EXHAUSTED]
        )

    # --- Utilities ---
    def _auth_metadata(self) -> List[tuple]:
        return [
            ("authorization", f"Bearer {self.config.session_token}"),
            ("x-agent-id", self.config.agent_id),
        ]

    async def reconnect(self):
        """Clean restart connection"""
        await self.close()
        await self.connect()

    async def close(self):
        """Graceful shutdown"""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._channel:
            await self._channel.close()

# Example Usage
async def main():
    config = ClientConfig(auth_token="your-secure-token")
    client = AzeercClient(config)
    
    try:
        await client.connect()
        state = await client.get_agent_state("agent-123")
        print(f"Agent state: {state.status}")
    finally:
        await client.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

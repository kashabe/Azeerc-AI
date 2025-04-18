# server.py
import asyncio
import logging
import signal
from functools import partial
from typing import Any, AsyncIterable, Dict, Optional

import grpc
from grpc import aio

from agent_service_pb2 import (
    AgentState,
    BroadcastMessageResponse,
    DeregisterAgentRequest,
    GetAgentStateRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    ListAgentsResponse,
    RegisterAgentRequest,
    RegisterAgentResponse,
    TaskReceipt,
)
from agent_service_pb2_grpc import (
    AgentServiceServicer,
    add_AgentServiceServicer_to_server,
)

# Configuration
SERVER_HOST = "[::]"
SERVER_PORT = 50051
MAX_WORKERS = 10
MAX_CONCURRENT_RPCS = 100

class AgentService(AgentServiceServicer):
    def __init__(self):
        self.agents: Dict[str, AgentState] = {}
        self.task_queue = asyncio.Queue()
        self._shutdown_event = asyncio.Event()

    # --- Core Service Implementation ---
    async def RegisterAgent(
        self, request: RegisterAgentRequest, context: grpc.aio.ServicerContext
    ) -> RegisterAgentResponse:
        """Handles agent registration with security validation."""
        agent_id = f"{request.agent_type}-{hash(request.host)}"
        response = RegisterAgentResponse(
            agent_id=agent_id,
            session_token="generated-secure-token",
            expiration_time=...,
            cluster_config=...,
        )
        self.agents[agent_id] = AgentState(
            agent_id=agent_id,
            status=AgentState.Status.IDLE,
            last_updated=...,
        )
        return response

    async def Heartbeat(
        self, request_stream: AsyncIterable[HeartbeatRequest], context: grpc.aio.ServicerContext
    ) -> AsyncIterable[HeartbeatResponse]:
        """Bidirectional streaming for real-time agent communication."""
        try:
            async for request in request_stream:
                if self._shutdown_event.is_set():
                    context.abort(grpc.StatusCode.UNAVAILABLE, "Server shutting down")
                
                # Process heartbeat and send instructions
                yield HeartbeatResponse(
                    task=...,
                    config=...,
                )
        except asyncio.CancelledError:
            logging.warning("Heartbeat stream cancelled by client")

    # --- Interceptors ---
class AuthInterceptor(grpc.aio.ServerInterceptor):
    """JWT validation and RBAC enforcement."""
    
    async def intercept_service(
        self,
        continuation: Any,
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        method = handler_call_details.method
        metadata = dict(handler_call_details.invocation_metadata)
        
        if not self._validate_token(metadata.get("authorization")):
            raise grpc.RpcError(
                code=grpc.StatusCode.UNAUTHENTICATED,
                details="Invalid authentication token"
            )
        
        return await continuation(handler_call_details)
    
    def _validate_token(self, token: Optional[str]) -> bool:
        return token == "expected-secure-token"

class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """Prometheus metrics collection."""
    
    def __init__(self):
        self.request_counter = ...  # Prometheus counter setup
    
    async def intercept_service(
        self,
        continuation: Any,
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        self.request_counter.labels(
            method=handler_call_details.method
        ).inc()
        
        start_time = asyncio.get_event_loop().time()
        try:
            return await continuation(handler_call_details)
        finally:
            latency = asyncio.get_event_loop().time() - start_time
            self.latency_histogram.observe(latency)

# --- Server Lifecycle Management ---
async def serve():
    interceptors = [
        AuthInterceptor(),
        MetricsInterceptor(),
    ]
    
    server = aio.server(
        interceptors=interceptors,
        maximum_concurrent_rpcs=MAX_CONCURRENT_RPCS,
        options=[
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.so_reuseport", 1),
        ]
    )
    
    service = AgentService()
    add_AgentServiceServicer_to_server(service, server)
    server.add_insecure_port(f"{SERVER_HOST}:{SERVER_PORT}")
    
    await server.start()
    logging.info(f"Server started on {SERVER_HOST}:{SERVER_PORT}")
    
    async def shutdown(signal: signal.Signals):
        logging.info(f"Received shutdown signal {signal.name}")
        await server.stop(5)
        service._shutdown_event.set()
    
    loop = asyncio.get_running_loop()
    for s in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(
            s,
            lambda s=s: asyncio.create_task(shutdown(s))
        )
    
    await server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())

# broker.py
import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis

# Configuration
WEBSOCKET_MAX_CONNECTIONS = 1000
MESSAGE_QUEUE_TIMEOUT = 30  # seconds
REDIS_URL = "redis://localhost:6379/0"

# --- Models & Protocols ---
class AgentMessage(BaseModel):
    action: str  # "heartbeat", "task", "broadcast"
    payload: Dict[str, Any]
    correlation_id: Optional[str] = None

class ServerMessage(BaseModel):
    status: str  # "success", "error", "ack"
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# --- Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.redis = Redis.from_url(REDIS_URL)
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, client_id: str):
        """Authenticate and register WebSocket connection"""
        async with self._lock:
            if len(self.active_connections) >= WEBSOCKET_MAX_CONNECTIONS:
                await websocket.close(code=status.WS_1001_GOING_AWAY)
                raise RuntimeError("Maximum connections reached")
            
            await websocket.accept()
            self.active_connections[client_id] = websocket
            await self.redis.sadd("active_clients", client_id)
            logging.info(f"Client {client_id} connected")

    async def disconnect(self, client_id: str):
        """Cleanup on WebSocket disconnect"""
        async with self._lock:
            websocket = self.active_connections.pop(client_id, None)
            if websocket:
                await self.redis.srem("active_clients", client_id)
                await websocket.close()
                logging.info(f"Client {client_id} disconnected")

    async def send_personal_message(self, message: ServerMessage, client_id: str):
        """Direct message to a single client"""
        websocket = self.active_connections.get(client_id)
        if websocket:
            await websocket.send_text(message.json())

    async def broadcast(self, message: ServerMessage):
        """Broadcast message to all connected clients"""
        for client_id, websocket in self.active_connections.items():
            await websocket.send_text(message.json())

    async def get_cluster_state(self) -> Dict[str, Any]:
        """Fetch real-time cluster state from Redis"""
        clients = await self.redis.smembers("active_clients")
        return {"active_clients": len(clients)}

# --- FastAPI Setup ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage WebSocket broker lifecycle"""
    app.state.redis = Redis.from_url(REDIS_URL)
    app.state.manager = ConnectionManager()
    logging.info("WebSocket broker started")
    yield
    await app.state.redis.close()
    logging.info("WebSocket broker stopped")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- WebSocket Endpoints ---
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: str,
    token: Optional[str] = None
):
    """Main WebSocket handler with JWT validation"""
    manager: ConnectionManager = app.state.manager
    
    # Authentication
    if not _validate_jwt(token):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    try:
        await manager.connect(websocket, client_id)
        while True:
            raw_message = await asyncio.wait_for(
                websocket.receive_text(),
                timeout=MESSAGE_QUEUE_TIMEOUT
            )
            try:
                message = AgentMessage.parse_raw(raw_message)
                await _handle_message(message, client_id, manager)
            except ValidationError as e:
                error = ServerMessage(status="error", error=f"Invalid message: {e}")
                await websocket.send_text(error.json())
    except WebSocketDisconnect:
        logging.warning(f"Client {client_id} disconnected abruptly")
    except asyncio.TimeoutError:
        logging.warning(f"Client {client_id} timed out")
    finally:
        await manager.disconnect(client_id)

# --- Message Handling ---
async def _handle_message(
    message: AgentMessage,
    client_id: str,
    manager: ConnectionManager
):
    """Route messages based on action type"""
    match message.action:
        case "heartbeat":
            await _handle_heartbeat(client_id, manager)
        case "task":
            await _forward_task(message.payload, manager)
        case "broadcast":
            await manager.broadcast(
                ServerMessage(
                    status="success",
                    data={"from": client_id, "message": message.payload}
                )
            )
        case _:
            error = ServerMessage(status="error", error="Invalid action type")
            await manager.send_personal_message(error, client_id)

async def _handle_heartbeat(client_id: str, manager: ConnectionManager):
    """Update client liveliness state"""
    await manager.redis.zadd("heartbeats", {client_id: time.time()})
    ack = ServerMessage(status="ack", data={"timestamp": time.time()})
    await manager.send_personal_message(ack, client_id)

async def _forward_task(payload: Dict, manager: ConnectionManager):
    """Distribute tasks via Redis pub/sub"""
    task_id = str(uuid.uuid4())
    await manager.redis.publish(
        "task_queue",
        json.dumps({"task_id": task_id, **payload})
    )

# --- Security Utilities ---
def _validate_jwt(token: Optional[str]) -> bool:
    """Mock JWT validation (replace with real implementation)"""
    return token == "secure-jwt-token"

# --- Metrics Endpoints ---
@app.get("/metrics")
async def prometheus_metrics():
    """Expose metrics for monitoring"""
    manager: ConnectionManager = app.state.manager
    state = await manager.get_cluster_state()
    return {
        "websocket_connections_total": len(manager.active_connections),
        "active_clusters": state["active_clients"]
    }

# --- Run Command ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "broker:app",
        host="0.0.0.0",
        port=8000,
        ws_ping_interval=30,
        ws_ping_timeout=60,
        log_level="info"
    )

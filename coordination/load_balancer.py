# load_balancer.py
import asyncio
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

# Constants
HEALTH_CHECK_INTERVAL = 30  # seconds
DECAY_FACTOR = 0.95         # For EWMA calculations

class LoadBalancerType(Enum):
    WEIGHTED_ROUND_ROBIN = "weighted_rr"
    WEIGHTED_LEAST_CONN = "weighted_least_conn"

class NodeStatus(Enum):
    HEALTHY = 1
    UNHEALTHY = 2
    DRAINING = 3

@dataclass
class BackendNode:
    node_id: str
    weight: int
    address: str
    current_connections: int = 0
    response_time_ewma: float = 0.0
    last_health_check: float = 0.0
    status: NodeStatus = NodeStatus.HEALTHY

@dataclass
class LoadBalancerConfig:
    algorithm: LoadBalancerType = LoadBalancerType.WEIGHTED_ROUND_ROBIN
    max_retries: int = 3
    sticky_sessions: bool = False
    panic_threshold: float = 0.2  # Enable panic mode if <20% nodes healthy

class LoadBalancer:
    def __init__(self, service_name: str, config: LoadBalancerConfig):
        self.service_name = service_name
        self.config = config
        self.nodes: Dict[str, BackendNode] = {}
        self.lock = asyncio.Lock()
        self.health_check_task = None
        self.logger = logging.getLogger(f"LoadBalancer[{service_name}]")
        
        # For round-robin state
        self.rr_counter: int = 0
        
        # For sticky sessions
        self.session_map: Dict[str, str] = {}

    async def start(self) -> None:
        """Initialize health check loop"""
        self.health_check_task = asyncio.create_task(self._health_check_loop())

    async def stop(self) -> None:
        """Graceful shutdown"""
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

    async def update_nodes(self, new_nodes: List[BackendNode]) -> None:
        """Update backend nodes from service discovery"""
        async with self.lock:
            current_ids = set(self.nodes.keys())
            new_ids = {n.node_id for n in new_nodes}
            
            # Remove departed nodes
            for node_id in current_ids - new_ids:
                del self.nodes[node_id]
            
            # Add/update nodes
            for node in new_nodes:
                if node.node_id in self.nodes:
                    self.nodes[node.node_id].weight = node.weight
                    self.nodes[node.node_id].address = node.address
                else:
                    self.nodes[node.node_id] = node

    async def select_node(self, session_id: Optional[str] = None) -> Optional[BackendNode]:
        """Select backend node based on algorithm"""
        async with self.lock:
            # Sticky session handling
            if self.config.sticky_sessions and session_id:
                if session_id in self.session_map:
                    node_id = self.session_map[session_id]
                    if node := self.nodes.get(node_id):
                        if node.status == NodeStatus.HEALTHY:
                            return node
            
            # Filter healthy nodes
            candidates = [
                node for node in self.nodes.values()
                if node.status == NodeStatus.HEALTHY
            ]
            
            if not candidates:
                return None
            
            # Apply panic mode if needed
            if len(candidates) / len(self.nodes) < self.config.panic_threshold:
                self.logger.warning("Entering panic mode - using all nodes")
                candidates = list(self.nodes.values())

            # Select algorithm
            if self.config.algorithm == LoadBalancerType.WEIGHTED_ROUND_ROBIN:
                return self._weighted_round_robin(candidates)
            elif self.config.algorithm == LoadBalancerType.WEIGHTED_LEAST_CONN:
                return self._weighted_least_connections(candidates)
            else:
                raise ValueError("Unsupported algorithm")

    def _weighted_round_robin(self, candidates: List[BackendNode]) -> BackendNode:
        """Weighted Round Robin algorithm implementation"""
        total_weight = sum(node.weight for node in candidates)
        self.rr_counter = (self.rr_counter + 1) % total_weight
        
        current = 0
        for node in candidates:
            current += node.weight
            if self.rr_counter < current:
                return node
        return candidates[-1]

    def _weighted_least_connections(self, candidates: List[BackendNode]) -> BackendNode:
        """Weighted Least Connections algorithm implementation"""
        return min(
            candidates,
            key=lambda node: node.current_connections / node.weight if node.weight > 0 else float('inf')
        )

    async def _health_check_loop(self) -> None:
        """Continuous health checking of backend nodes"""
        while True:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            async with self.lock:
                for node in self.nodes.values():
                    try:
                        # Simulated health check (replace with actual TCP/HTTP check)
                        is_healthy = await self._perform_health_check(node)
                        node.status = NodeStatus.HEALTHY if is_healthy else NodeStatus.UNHEALTHY
                        node.last_health_check = time.time()
                    except Exception as e:
                        self.logger.error(f"Health check failed for {node.node_id}: {str(e)}")
                        node.status = NodeStatus.UNHEALTHY

    async def _perform_health_check(self, node: BackendNode) -> bool:
        """Actual health check implementation"""
        # Example: HTTP health check
        # async with aiohttp.ClientSession() as session:
        #     async with session.get(f"http://{node.address}/health") as resp:
        #         return resp.status == 200
        return random.random() < 0.95  # Simulated 95% success rate

    async def register_metrics(self, conn_time: float) -> None:
        """Update performance metrics (call after request completion)"""
        async with self.lock:
            # Update EWMA response times
            for node in self.nodes.values():
                if node.response_time_ewma == 0:
                    node.response_time_ewma = conn_time
                else:
                    node.response_time_ewma = (DECAY_FACTOR * node.response_time_ewma + 
                                              (1 - DECAY_FACTOR) * conn_time)

class LoadBalancerClient:
    def __init__(self, lb: LoadBalancer):
        self.lb = lb
        self.retries = 0

    async def send_request(self, data: bytes, session_id: Optional[str] = None) -> bytes:
        """Send request through load balancer with retries"""
        while self.retries < self.lb.config.max_retries:
            node = await self.lb.select_node(session_id)
            if not node:
                raise ConnectionError("No available backend nodes")
            
            start_time = time.monotonic()
            try:
                # Simulated request (replace with actual network call)
                result = await self._call_backend(node, data)
                await self.lb.register_metrics(time.monotonic() - start_time)
                return result
            except Exception as e:
                self.retries += 1
                self.lb.logger.error(f"Request to {node.node_id} failed: {str(e)}")
                continue
        raise ConnectionError(f"Max retries ({self.lb.config.max_retries}) exceeded")

    async def _call_backend(self, node: BackendNode, data: bytes) -> bytes:
        """Actual backend call implementation"""
        # Example: aiohttp request
        # async with aiohttp.ClientSession() as session:
        #     async with session.post(node.address, data=data) as resp:
        #         return await resp.read()
        await asyncio.sleep(random.expovariate(1.0))  # Simulated latency
        if random.random() < 0.05:  # Simulated 5% error rate
            raise ConnectionError("Backend request failed")
        return b"mock_response"

# Example Usage
async def main():
    logging.basicConfig(level=logging.INFO)
    
    # Initialize load balancer
    config = LoadBalancerConfig(
        algorithm=LoadBalancerType.WEIGHTED_LEAST_CONN,
        sticky_sessions=True
    )
    lb = LoadBalancer("ai-inference", config)
    
    # Mock nodes
    await lb.update_nodes([
        BackendNode(node_id="node1", weight=3, address="10.0.0.1:8000"),
        BackendNode(node_id="node2", weight=2, address="10.0.0.2:8000"),
        BackendNode(node_id="node3", weight=1, address="10.0.0.3:8000"),
    ])
    
    await lb.start()
    
    # Simulate requests
    client = LoadBalancerClient(lb)
    for _ in range(10):
        try:
            response = await client.send_request(b"test_data", session_id="session_123")
            print(f"Received response: {response.decode()}")
        except Exception as e:
            print(f"Request failed: {str(e)}")
    
    await lb.stop()

if __name__ == "__main__":
    asyncio.run(main())

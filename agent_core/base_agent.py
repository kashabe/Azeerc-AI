# base_agent.py
from __future__ import annotations
import abc
import logging
from enum import Enum, auto
from typing import Optional, TypeVar, Generic, Dict, Any
from dataclasses import dataclass
import threading
import time

# Type definitions
AgentConfigType = TypeVar('AgentConfigType')
StateType = TypeVar('StateType')

class AgentState(Enum):
    """Lifecycle states of an agent."""
    CREATED = auto()
    INITIALIZED = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPED = auto()
    ERROR = auto()

@dataclass
class BaseAgentConfig:
    """Base configuration dataclass for agent initialization."""
    agent_id: str
    heartbeat_interval: int = 30  # seconds
    max_retries: int = 3
    log_level: str = "INFO"

class AgentInitializationError(Exception):
    """Critical failure during agent initialization."""
    def __init__(self, message: str, error_code: int):
        super().__init__(f"[{error_code}] {message}")
        self.error_code = error_code

class AgentExecutionError(Exception):
    """Runtime error during agent operation."""
    pass

class BaseAgent(Generic[AgentConfigType, StateType]):
    """Abstract base class for all Azeerc AI agents."""
    
    def __init__(self, config: AgentConfigType):
        self._state: AgentState = AgentState.CREATED
        self._config: AgentConfigType = config
        self._logger: logging.Logger = self._configure_logger()
        self._state_lock: threading.Lock = threading.Lock()
        self._last_heartbeat: float = 0.0
        self._plugins: Dict[str, Any] = {}
        
        # Initialize communication channels (to be extended)
        self._message_queue = None  
        self._rpc_server = None

    def _configure_logger(self) -> logging.Logger:
        """Initialize agent-specific logger."""
        logger = logging.getLogger(f"Agent.{self._config.agent_id}")
        logger.setLevel(self._config.log_level)
        return logger

    @property
    def state(self) -> AgentState:
        """Current state of the agent (thread-safe)."""
        with self._state_lock:
            return self._state

    @state.setter
    def state(self, new_state: AgentState) -> None:
        """Update agent state with thread safety."""
        with self._state_lock:
            allowed_transitions = {
                AgentState.CREATED: [AgentState.INITIALIZED, AgentState.ERROR],
                AgentState.INITIALIZED: [AgentState.RUNNING, AgentState.STOPPED],
                AgentState.RUNNING: [AgentState.PAUSED, AgentState.STOPPED, AgentState.ERROR],
                AgentState.PAUSED: [AgentState.RUNNING, AgentState.STOPPED],
                AgentState.STOPPED: [],
                AgentState.ERROR: [AgentState.STOPPED]
            }
            if new_state not in allowed_transitions[self._state]:
                raise ValueError(f"Illegal state transition: {self._state} -> {new_state}")
            self._state = new_state
            self._logger.info(f"State changed to {new_state.name}")

    def initialize(self) -> None:
        """Prepare agent resources and validate configuration."""
        if self.state != AgentState.CREATED:
            raise AgentInitializationError("Agent already initialized", 1001)
            
        try:
            self._load_plugins()
            self._init_communication()
            self.state = AgentState.INITIALIZED
        except Exception as e:
            self.state = AgentState.ERROR
            raise AgentInitializationError(str(e), 1002) from e

    def start(self) -> None:
        """Activate agent's main operational loop."""
        if self.state != AgentState.INITIALIZED:
            raise AgentExecutionError("Agent must be initialized before starting")
            
        self.state = AgentState.RUNNING
        self._logger.info("Starting main execution loop")
        
        # Start background threads
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()
        threading.Thread(target=self._message_processor, daemon=True).start()

    def stop(self) -> None:
        """Gracefully terminate agent operations."""
        self.state = AgentState.STOPPED
        self._cleanup_resources()

    def pause(self) -> None:
        """Temporarily suspend agent activities."""
        if self.state != AgentState.RUNNING:
            raise AgentExecutionError("Agent must be running to pause")
        self.state = AgentState.PAUSED

    def resume(self) -> None:
        """Resume agent operations after pause."""
        if self.state != AgentState.PAUSED:
            raise AgentExecutionError("Agent must be paused to resume")
        self.state = AgentState.RUNNING

    @abc.abstractmethod
    def execute_task(self, task_input: Any) -> Any:
        """Abstract method to be implemented by concrete agents."""
        pass

    def _load_plugins(self) -> None:
        """Load enabled plugins from configuration."""
        # Implementation placeholder
        self._logger.debug("Loading plugins...")

    def _init_communication(self) -> None:
        """Initialize message queues and RPC endpoints."""
        # Implementation placeholder (gRPC/MQTT/WebSocket)
        self._logger.debug("Initializing communication channels...")

    def _heartbeat_monitor(self) -> None:
        """Background thread for health monitoring."""
        while self.state != AgentState.STOPPED:
            if self.state == AgentState.RUNNING:
                self._last_heartbeat = time.time()
                self._report_health()
            time.sleep(self._config.heartbeat_interval)

    def _message_processor(self) -> None:
        """Background thread for processing incoming messages."""
        while self.state != AgentState.STOPPED:
            if self.state == AgentState.RUNNING and self._message_queue:
                # Implementation placeholder
                time.sleep(0.1)

    def _report_health(self) -> None:
        """Report agent health status to central monitoring."""
        # Implementation placeholder
        self._logger.debug("Sending heartbeat...")

    def _cleanup_resources(self) -> None:
        """Release network connections and external resources."""
        self._logger.info("Cleaning up resources...")
        # Implementation placeholder

# Example Usage
class SampleAgentConfig(BaseAgentConfig):
    """Concrete configuration for demonstration."""
    additional_param: str = "default"

class SampleAgent(BaseAgent[SampleAgentConfig, None]):
    """Concrete agent implementation example."""
    
    def execute_task(self, task_input: str) -> str:
        if self.state != AgentState.RUNNING:
            raise AgentExecutionError("Agent not in running state")
        return f"Processed: {task_input}"

if __name__ == "__main__":
    # Initialize and run a sample agent
    config = SampleAgentConfig(agent_id="test_agent_001")
    agent = SampleAgent(config)
    
    try:
        agent.initialize()
        agent.start()
        result = agent.execute_task("test_input")
        print(f"Task result: {result}")
    finally:
        agent.stop()

# environment.py
import logging
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gymnasium as gym
import numpy as np
from gymnasium import spaces
from pydantic import BaseModel, ValidationError, validator

logger = logging.getLogger("TaskSchedulingEnv")

@dataclass(frozen=True)
class Task:
    task_id: str
    resource_demand: Dict[str, float]  # {"cpu": 2.5, "gpu": 1}
    priority: int
    deadline: float  # Seconds since epoch
    creation_time: float = time.time()

@dataclass
class Resource:
    node_id: str
    capacity: Dict[str, float]
    allocated: Dict[str, float] = field(default_factory=dict)
    last_updated: float = time.time()

class EnvState(BaseModel):
    cluster_resources: List[Resource]
    pending_tasks: List[Task]
    running_tasks: Dict[str, Tuple[Task, str]]  # task_id -> (Task, node_id)
    system_load: Dict[str, float]
    timestamp: float

    @validator("cluster_resources")
    def check_resource_capacity(cls, v):
        for r in v:
            if any(alloc > cap for alloc, cap in zip(r.allocated.values(), r.capacity.values())):
                raise ValueError(f"Over-allocated resource {r.node_id}")
        return v

class TaskSchedulingEnv(gym.Env):
    """Multi-agent task scheduling environment with dynamic workloads"""
    
    metadata = {"render_modes": ["human", "ansi"], "version": "2.1.0"}
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        
        # Configuration
        self.config = config or {
            "max_tasks": 1000,
            "resource_types": ["cpu", "gpu", "memory"],
            "episode_length": 500,
            "reward_weights": {
                "utilization": 0.6,
                "sla_violation": -1.0,
                "priority": 0.3,
                "overload_penalty": -2.0
            }
        }
        
        # Observation Space: Dict of Box spaces for cluster state
        self.observation_space = spaces.Dict({
            "resource_states": spaces.Box(
                low=0, high=1, shape=(len(self.config["resource_types"]),), dtype=np.float32
            ),
            "task_queue": spaces.Box(
                low=0, high=np.inf, shape=(self.config["max_tasks"], 3), dtype=np.float32
            )
        })
        
        # Action Space: MultiDiscrete for node selection per task
        self.action_space = spaces.MultiDiscrete(
            [len(self.config["resource_types"]) + 1] * self.config["max_tasks"]
        )
        
        # Initial state
        self.current_state: Optional[EnvState] = None
        self.current_step: int = 0
        self._task_counter: int = 0
        self._resource_pool: List[Resource] = self._init_resources()
        self._metrics = {
            "tasks_completed": 0,
            "sla_violations": 0,
            "resource_overloads": 0
        }

    def reset(self, *, seed: Optional[int] = None, options: Optional[Dict] = None) -> Tuple[EnvState, Dict]:
        super().reset(seed=seed)
        
        # Initialize clean state
        self._resource_pool = self._init_resources()
        self.current_state = EnvState(
            cluster_resources=self._resource_pool,
            pending_tasks=[],
            running_tasks={},
            system_load={rt: 0.0 for rt in self.config["resource_types"]},
            timestamp=time.time()
        )
        self.current_step = 0
        self._task_counter = 0
        self._metrics.clear()
        
        # Generate initial workload
        for _ in range(random.randint(10, 50)):
            self._generate_new_task()
            
        return self._get_obs(), self._get_info()

    def step(self, action: np.ndarray) -> Tuple[EnvState, float, bool, bool, Dict]:
        """Execute scheduling decisions and progress simulation"""
        if self.current_state is None:
            raise RuntimeError("Environment not initialized. Call reset() first.")
            
        reward = 0.0
        terminated = False
        
        # 1. Process current assignments from action
        self._apply_scheduling_decisions(action)
        
        # 2. Progress simulation (e.g. task completions)
        completed = self._process_running_tasks()
        reward += self._calculate_completion_reward(completed)
        
        # 3. Generate new tasks
        new_tasks = self._generate_workload()
        
        # 4. Check termination
        self.current_step += 1
        if self.current_step >= self.config["episode_length"]:
            terminated = True
            
        # 5. Calculate final metrics
        reward += self._calculate_utilization_reward()
        reward += self._calculate_sla_penalty()
        
        return self._get_obs(), reward, terminated, False, self._get_info()

    def render(self, mode="human"):
        """Render cluster state visualization"""
        if mode == "human":
            print(f"Step {self.current_step}")
            print(f"Nodes: {len(self.current_state.cluster_resources)}")
            print(f"Pending Tasks: {len(self.current_state.pending_tasks)}")
            print(f"Utilization: {self.current_state.system_load}")
            print(f"Metrics: {self._metrics}")
        elif mode == "ansi":
            return self._get_obs()

    def close(self):
        self.current_state = None

    def _init_resources(self) -> List[Resource]:
        """Initialize cluster resources with randomized capacities"""
        return [
            Resource(
                node_id=f"node-{i}",
                capacity={
                    rt: random.uniform(4.0, 16.0)
                    for rt in self.config["resource_types"]
                }
            ) for i in range(10)
        ]

    def _get_obs(self) -> Dict[str, Any]:
        """Convert state to observation tensor"""
        resource_states = []
        for res in self.current_state.cluster_resources:
            util = [
                res.allocated.get(rt, 0.0) / res.capacity[rt]
                for rt in self.config["resource_types"]
            ]
            resource_states.extend(util)
            
        task_features = []
        for task in self.current_state.pending_tasks[:self.config["max_tasks"]]:
            task_features.append([
                task.priority / 10.0,
                (task.deadline - time.time()) / 3600.0,
                sum(task.resource_demand.values())
            ])
        
        return {
            "resource_states": np.array(resource_states, dtype=np.float32),
            "task_queue": np.array(task_features, dtype=np.float32)
        }

    def _get_info(self) -> Dict[str, Any]:
        return {
            "current_step": self.current_step,
            "system_load": self.current_state.system_load,
            **self._metrics
        }

    def _generate_new_task(self):
        """Create new task with randomized requirements"""
        self._task_counter += 1
        task = Task(
            task_id=f"task-{self._task_counter}",
            resource_demand={
                rt: random.uniform(0.1, 4.0)
                for rt in random.sample(
                    self.config["resource_types"], 
                    k=random.randint(1, len(self.config["resource_types"]))
                )
            },
            priority=random.randint(1, 5),
            deadline=time.time() + random.uniform(60.0, 3600.0)
        )
        self.current_state.pending_tasks.append(task)

    def _apply_scheduling_decisions(self, action: np.ndarray):
        """Assign tasks to nodes based on agent's action"""
        for task_idx, node_idx in enumerate(action):
            if task_idx >= len(self.current_state.pending_tasks):
                continue
                
            task = self.current_state.pending_tasks[task_idx]
            if node_idx >= len(self.current_state.cluster_resources):
                self._metrics["sla_violations"] += 1
                continue
                
            target_node = self.current_state.cluster_resources[node_idx]
            if self._can_allocate(task, target_node):
                self._allocate_task(task, target_node)
                self.current_state.pending_tasks.pop(task_idx)
            else:
                self._metrics["resource_overloads"] += 1

    def _can_allocate(self, task: Task, node: Resource) -> bool:
        """Check if node can accommodate task requirements"""
        return all(
            node.allocated.get(rt, 0.0) + task.resource_demand.get(rt, 0.0) <= node.capacity[rt]
            for rt in task.resource_demand.keys()
        )

    def _allocate_task(self, task: Task, node: Resource):
        """Commit resources to task"""
        for rt, demand in task.resource_demand.items():
            node.allocated[rt] = node.allocated.get(rt, 0.0) + demand
        self.current_state.running_tasks[task.task_id] = (task, node.node_id)
        node.last_updated = time.time()

    def _process_running_tasks(self) -> List[Task]:
        """Simulate task execution and completion"""
        completed = []
        for task_id in list(self.current_state.running_tasks.keys()):
            task, node_id = self.current_state.running_tasks[task_id]
            node = next(n for n in self.current_state.cluster_resources if n.node_id == node_id)
            
            # Simulate completion
            if random.random() < 0.25:  # 25% chance per step
                self._release_resources(task, node)
                completed.append(task)
                self.current_state.running_tasks.pop(task_id)
                self._metrics["tasks_completed"] += 1
        return completed

    def _release_resources(self, task: Task, node: Resource):
        """Free up allocated resources"""
        for rt in task.resource_demand.keys():
            node.allocated[rt] -= task.resource_demand[rt]

    def _calculate_completion_reward(self, completed: List[Task]) -> float:
        """Calculate reward from successfully completed tasks"""
        reward = 0.0
        for task in completed:
            time_taken = time.time() - task.creation_time
            sla = task.deadline - task.creation_time
            reward += self.config["reward_weights"]["priority"] * task.priority
            if time_taken > sla:
                reward += self.config["reward_weights"]["sla_violation"]
        return reward

    def _calculate_utilization_reward(self) -> float:
        """Reward based on overall cluster utilization"""
        total_util = 0.0
        for node in self.current_state.cluster_resources:
            node_util = sum(
                alloc / cap 
                for rt, (alloc, cap) in zip(
                    node.allocated.values(), 
                    node.capacity.values()
                )
            ) / len(node.capacity)
            total_util += node_util
        avg_util = total_util / len(self.current_state.cluster_resources)
        return avg_util * self.config["reward_weights"]["utilization"]

    def _calculate_sla_penalty(self) -> float:
        """Penalize tasks missing deadlines"""
        now = time.time()
        violations = 0
        for task in self.current_state.pending_tasks + list(self.current_state.running_tasks.values()):
            if now > task.deadline:
                violations += 1
        return violations * self.config["reward_weights"]["sla_violation"]

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    env = TaskSchedulingEnv()
    obs, info = env.reset()
    
    for step in range(100):
        action = env.action_space.sample()  # Replace with agent policy
        obs, reward, terminated, truncated, info = env.step(action)
        
        if terminated or truncated:
            print(f"Episode finished after {step+1} steps")
            break

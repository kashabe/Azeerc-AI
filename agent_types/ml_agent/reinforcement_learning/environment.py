# environment.py
import logging
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import gymnasium as gym
import numpy as np
from gymnasium import spaces

logger = logging.getLogger("AzeercEnv")

@dataclass
class EnvironmentConfig:
    num_agents: int = 3
    grid_size: Tuple[int, int] = (10, 10)
    max_steps: int = 100
    enable_rendering: bool = False
    reward_mode: str = "collaborative"  # competitive/mixed
    seed: Optional[int] = None

class AzeercMultiAgentEnv(gym.Env):
    metadata = {"render_modes": ["human", "rgb_array"], "render_fps": 30}

    def __init__(self, config: EnvironmentConfig):
        super().__init__()
        self.config = config
        self._validate_config()

        # Observation space: Dict of agent observations
        self.observation_space = spaces.Dict({
            f"agent_{i}": spaces.Dict({
                "position": spaces.Box(low=0, high=max(self.grid_size), shape=(2,), dtype=int),
                "resources": spaces.Box(low=0, high=100, shape=(1,), dtype=float),
                "global_map": spaces.MultiBinary(self.grid_size)
            }) for i in range(self.config.num_agents)
        })

        # Action space: Dict of discrete actions
        self.action_space = spaces.Dict({
            f"agent_{i}": spaces.Discrete(4)  # 0:up, 1:down, 2:left, 3:right
            for i in range(self.config.num_agents)
        })

        # State tracking
        self.current_step = 0
        self.agent_positions = {}
        self.agent_resources = {}
        self.global_resource_map = np.zeros(self.grid_size, dtype=float)
        
        # Rendering components
        self.renderer = None
        if self.config.enable_rendering:
            self._init_rendering()

    def _validate_config(self):
        if self.config.num_agents < 1:
            raise ValueError("At least 1 agent required")
        if min(self.grid_size) < 5:
            raise ValueError("Grid size too small")
        if self.config.reward_mode not in ["collaborative", "competitive", "mixed"]:
            raise ValueError("Invalid reward mode")

    def reset(self, **kwargs) -> Dict:
        """Reset environment to initial state"""
        super().reset(**kwargs)
        self.current_step = 0
        
        # Initialize agent states
        for agent_id in range(self.config.num_agents):
            self.agent_positions[f"agent_{agent_id}"] = (
                random.randint(0, self.grid_size[0]-1),
                random.randint(0, self.grid_size[1]-1)
            )
            self.agent_resources[f"agent_{agent_id}"] = 0.0
        
        # Generate resource distribution
        self.global_resource_map = self._generate_resource_map()
        
        return self._get_observations(), self._get_info()

    def step(self, actions: Dict) -> Tuple[Dict, Dict, bool, Dict]:
        """Execute one timestep for all agents"""
        self.current_step += 1
        rewards = {agent_id: 0.0 for agent_id in self.agent_positions}
        terminated = False
        truncated = self.current_step >= self.config.max_steps
        
        # Process agent actions
        for agent_id, action in actions.items():
            new_pos = self._move_agent(agent_id, action)
            rewards[agent_id] += self._collect_resources(agent_id, new_pos)
        
        # Calculate global rewards
        if self.config.reward_mode == "collaborative":
            global_reward = sum(rewards.values()) / self.config.num_agents
            rewards = {k: global_reward for k in rewards}
        elif self.config.reward_mode == "competitive":
            total = sum(rewards.values())
            rewards = {k: (v - total/self.config.num_agents) for k, v in rewards.items()}
        
        # Check termination condition
        if np.sum(self.global_resource_map) < 0.1:
            terminated = True
            for agent_id in rewards:
                rewards[agent_id] += 10.0  # Completion bonus
        
        return self._get_observations(), rewards, terminated, truncated, self._get_info()

    def render(self):
        """Render environment state"""
        if not self.config.enable_rendering:
            return
        
        if self.renderer is None:
            self._init_rendering()
        
        self.renderer.draw(
            agent_positions=self.agent_positions,
            resource_map=self.global_resource_map
        )

    def close(self):
        """Clean up resources"""
        if self.renderer:
            self.renderer.close()

    def _init_rendering(self):
        """Lazy-load rendering components"""
        try:
            from .render import PyGameRenderer
            self.renderer = PyGameRenderer(self.grid_size)
        except ImportError:
            logger.warning("PyGame not installed, rendering disabled")
            self.config.enable_rendering = False

    def _generate_resource_map(self) -> np.ndarray:
        """Generate randomized resource distribution"""
        map = np.random.rand(*self.grid_size) * 100
        # Add resource clusters
        for _ in range(3):
            x = random.randint(0, self.grid_size[0]-1)
            y = random.randint(0, self.grid_size[1]-1)
            map[x:x+3, y:y+3] += np.random.rand(3,3)*500
        return np.clip(map, 0, 1000)

    def _move_agent(self, agent_id: str, action: int) -> Tuple[int, int]:
        """Update agent position with collision checking"""
        x, y = self.agent_positions[agent_id]
        dx, dy = 0, 0
        
        if action == 0: dx = -1  # up
        elif action == 1: dx = 1  # down
        elif action == 2: dy = -1  # left
        elif action == 3: dy = 1  # right
        
        new_x = np.clip(x + dx, 0, self.grid_size[0]-1)
        new_y = np.clip(y + dy, 0, self.grid_size[1]-1)
        
        # Check for agent collisions
        if (new_x, new_y) in self.agent_positions.values():
            return (x, y)  # Block movement
        
        self.agent_positions[agent_id] = (new_x, new_y)
        return (new_x, new_y)

    def _collect_resources(self, agent_id: str, position: Tuple[int, int]) -> float:
        """Collect resources at current position"""
        x, y = position
        resource_amount = self.global_resource_map[x, y]
        
        if resource_amount > 0:
            collected = min(resource_amount, 10.0)  # Max 10 per step
            self.global_resource_map[x, y] -= collected
            self.agent_resources[agent_id] += collected
            return collected * 0.5  # Reward coefficient
        return 0.0

    def _get_observations(self) -> Dict:
        """Build agent observations"""
        obs = {}
        for agent_id in self.agent_positions:
            x, y = self.agent_positions[agent_id]
            obs[agent_id] = {
                "position": np.array([x, y], dtype=int),
                "resources": np.array([self.agent_resources[agent_id]], dtype=float),
                "global_map": self.global_resource_map.copy()
            }
        return obs

    def _get_info(self) -> Dict:
        """Return debug info"""
        return {
            "step": self.current_step,
            "total_resources": np.sum(self.global_resource_map),
            "agent_positions": self.agent_positions.copy()
        }

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    config = EnvironmentConfig(
        num_agents=3,
        grid_size=(15, 15),
        max_steps=200,
        enable_rendering=True,
        reward_mode="collaborative",
        seed=42
    )
    
    env = AzeercMultiAgentEnv(config)
    obs, info = env.reset()
    
    for _ in range(config.max_steps):
        actions = {
            agent_id: env.action_space[agent_id].sample()
            for agent_id in obs.keys()
        }
        obs, rewards, terminated, truncated, info = env.step(actions)
        env.render()
        
        if terminated:
            break

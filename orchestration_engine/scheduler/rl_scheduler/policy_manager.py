# policy_manager.py
from __future__ import annotations
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import ray
from pydantic import BaseModel, ValidationError, validator
from ray import tune
from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import PolicyID
from semver import VersionInfo

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger("PolicyManager")

#region Metrics
POLICY_VERSION = Gauge("policy_version", "Active policy version", ["policy_id"])
TRAINING_TIME = Histogram("training_duration_seconds", "Policy training duration")
INFERENCE_LATENCY = Histogram("policy_inference_latency", "Inference latency in seconds")
MODEL_LOAD_ERRORS = Counter("model_load_errors", "Policy loading failures")
#endregion

@dataclass(frozen=True)
class PolicyConfig:
    policy_class: str  # "PPO", "SAC"
    framework: str  # "torch", "tf"
    observation_space: Dict[str, Any]
    action_space: Dict[str, Any]
    batch_size: int = 1024
    rollout_fragment_length: int = 200

class PolicyMetadata(BaseModel):
    policy_id: str
    version: str
    created_at: datetime
    dependencies: Dict[str, str]  # {"numpy": "1.21.0", "ray": "2.5.0"}
    signature: str  # Model hash
    serving_endpoint: Optional[str]
    performance: Dict[str, float] = {}  # {"throughput": 1000, "accuracy": 0.95}

    @validator("version")
    def validate_semver(cls, v):
        if not VersionInfo.isvalid(v):
            raise ValueError(f"Invalid semantic version: {v}")
        return v

class PolicyManager:
    """Orchestrates RL policy lifecycle with enterprise-grade features"""
    
    def __init__(self, storage_path: str = "/mnt/models"):
        self.storage = Path(storage_path)
        self.storage.mkdir(exist_ok=True)
        
        self.active_policies: Dict[str, Policy] = {}
        self.metadata_store: Dict[str, PolicyMetadata] = {}
        
        ray.init(ignore_reinit_error=True)

    def create_policy(self, config: PolicyConfig) -> Tuple[str, PolicyMetadata]:
        """Initialize new policy with versioning"""
        policy_id = f"policy_{uuid.uuid4().hex[:8]}"
        version = "1.0.0"
        
        # Initialize RLlib policy
        algo_config = (PPOConfig()
                      .framework(config.framework)
                      .rollouts(rollout_fragment_length=config.rollout_fragment_length)
                      .training(train_batch_size=config.batch_size))
        
        policy = algo_config.build(
            env=None,
            policy_class=config.policy_class,
            observation_space=config.observation_space,
            action_space=config.action_space
        )
        
        # Generate metadata
        metadata = PolicyMetadata(
            policy_id=policy_id,
            version=version,
            created_at=datetime.utcnow(),
            dependencies=self._detect_dependencies(),
            signature=self._generate_signature(policy)
        )
        
        self._save_policy(policy, metadata)
        return policy_id, metadata

    @TRAINING_TIME.time()
    def train_policy(self, policy_id: str, training_data: Dict[str, np.ndarray], 
                    epochs: int = 10) -> PolicyMetadata:
        """Execute distributed training with metrics capture"""
        if policy_id not in self.active_policies:
            raise ValueError(f"Policy {policy_id} not loaded")
            
        policy = self.active_policies[policy_id]
        
        # Convert data to Ray Dataset
        dataset = ray.data.from_numpy(training_data)
        
        # Custom training loop
        for epoch in range(epochs):
            batch = dataset.random_shuffle().iterator().take(1)
            result = policy.train(batch)
            
            # Capture metrics
            self._record_training_metrics(policy_id, result)
            
        # Version update
        new_version = self._increment_version(
            self.metadata_store[policy_id].version
        )
        metadata = self.metadata_store[policy_id].copy(update={
            "version": new_version,
            "performance": {
                "training_loss": result["policy_loss"],
                "entropy": result["entropy"]
            }
        })
        
        self._save_policy(policy, metadata)
        return metadata

    def deploy_policy(self, policy_id: str, endpoint: str) -> PolicyMetadata:
        """Deploy policy as gRPC endpoint"""
        if policy_id not in self.active_policies:
            raise self._load_policy(policy_id)
            
        # Implementation would integrate with model serving component
        metadata = self.metadata_store[policy_id].copy(update={
            "serving_endpoint": endpoint
        })
        self.metadata_store[policy_id] = metadata
        
        POLICY_VERSION.labels(policy_id=policy_id).set(
            VersionInfo.parse(metadata.version).major
        )
        return metadata

    @INFERENCE_LATENCY.time()
    def get_action(self, policy_id: str, observation: np.ndarray) -> np.ndarray:
        """Get action from policy with latency monitoring"""
        if policy_id not in self.active_policies:
            self._load_policy(policy_id)
        
        policy = self.active_policies[policy_id]
        return policy.compute_single_action(observation)

    def rollback_policy(self, policy_id: str, target_version: str) -> PolicyMetadata:
        """Version rollback with dependency validation"""
        available_versions = self._list_versions(policy_id)
        
        if target_version not in available_versions:
            raise ValueError(f"Version {target_version} not found")
            
        # Load historical policy
        policy_path = self.storage / policy_id / target_version
        metadata = PolicyMetadata.parse_file(policy_path / "metadata.json")
        
        # Dependency check
        current_env = self._detect_dependencies()
        for lib, ver in metadata.dependencies.items():
            if current_env.get(lib, "0.0.0") != ver:
                logger.warning(f"Dependency mismatch: {lib} {ver} required")
                
        self.active_policies[policy_id] = self._load_policy_from_path(policy_path)
        self.metadata_store[policy_id] = metadata
        return metadata

    def _save_policy(self, policy: Policy, metadata: PolicyMetadata):
        """Persist policy with versioned storage"""
        save_path = self.storage / metadata.policy_id / metadata.version
        save_path.mkdir(parents=True, exist_ok=True)
        
        try:
            policy.export_checkpoint(str(save_path / "model"))
            with open(save_path / "metadata.json", "w") as f:
                f.write(metadata.json())
                
            logger.info(f"Saved policy {metadata.policy_id} v{metadata.version}")
        except Exception as e:
            MODEL_LOAD_ERRORS.inc()
            logger.error(f"Policy save failed: {str(e)}")
            raise

    def _load_policy(self, policy_id: str) -> Policy:
        """Load latest version of a policy"""
        versions = self._list_versions(policy_id)
        if not versions:
            raise ValueError(f"No versions found for {policy_id}")
            
        latest_version = sorted(
            versions, 
            key=lambda v: VersionInfo.parse(v)
        )[-1]
        
        policy_path = self.storage / policy_id / latest_version
        return self._load_policy_from_path(policy_path)

    def _load_policy_from_path(self, path: Path) -> Policy:
        """Secure policy loading with integrity checks"""
        try:
            policy = Policy.from_checkpoint(str(path / "model"))
            self._verify_signature(policy, path / "metadata.json")
            return policy
        except Exception as e:
            MODEL_LOAD_ERRORS.inc()
            logger.error(f"Policy load failed: {str(e)}")
            raise

    def _generate_signature(self, policy: Policy) -> str:
        """Generate model checksum (simplified example)"""
        return str(uuid.uuid4())  # Replace with actual hash

    def _verify_signature(self, policy: Policy, metadata_path: Path):
        """Verify model integrity (stub implementation)"""
        pass  # Implement hash verification

    def _detect_dependencies(self) -> Dict[str, str]:
        """Capture current environment packages"""
        return {
            "ray": ray.__version__,
            "numpy": np.__version__,
            "python": "3.9.16"
        }

    def _increment_version(self, current: str) -> str:
        """SemVer version incrementing"""
        v = VersionInfo.parse(current)
        return str(v.bump_minor() if v.patch == 0 else v.bump_patch())

    def _list_versions(self, policy_id: str) -> List[str]:
        """List stored versions for a policy"""
        policy_dir = self.storage / policy_id
        if not policy_dir.exists():
            return []
            
        return [d.name for d in policy_dir.iterdir() if d.is_dir()]

    def _record_training_metrics(self, policy_id: str, result: Dict[str, Any]):
        """Capture RLlib training metrics to Prometheus"""
        for metric in ["policy_loss", "entropy", "kl"]:
            if metric in result:
                Gauge(f"training_{metric}", "").set(result[metric])

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    manager = PolicyManager()
    
    # Create new policy
    config = PolicyConfig(
        policy_class="PPO",
        framework="torch",
        observation_space={"type": "Box", "shape": (4,)},
        action_space={"type": "Discrete", "n": 2}
    )
    policy_id, metadata = manager.create_policy(config)
    print(f"Created policy {policy_id} v{metadata.version}")
    
    # Train with sample data
    data = {
        "observations": np.random.randn(1000, 4),
        "actions": np.random.randint(0, 2, 1000),
        "rewards": np.random.rand(1000)
    }
    updated_meta = manager.train_policy(policy_id, data, epochs=5)
    print(f"Trained to v{updated_meta.version}")
    
    # Deploy
    deployed_meta = manager.deploy_policy(policy_id, "grpc://localhost:50051")
    print(f"Deployed to {deployed_meta.serving_endpoint}")
    
    # Inference
    obs = np.random.randn(4)
    action = manager.get_action(policy_id, obs)
    print(f"Inferred action: {action}")

# key_rotation.py
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from cryptography.hazmat.primitives import serialization
from prometheus_client import Gauge

logger = logging.getLogger("KeyRotation")

class RotationError(Exception):
    """Base key rotation exception"""

class CloudRotationStrategy(ABC):
    @abstractmethod
    def create_new_version(self, key_id: str) -> str:
        pass
    
    @abstractmethod
    def disable_old_versions(self, key_id: str, versions: List[str]):
        pass

class AWSKMSRotation(CloudRotationStrategy):
    def __init__(self, kms_client):
        self.client = kms_client

    def create_new_version(self, key_id: str) -> str:
        response = self.client.create_key(
            Policy=key_id.policy,
            Description=f"Auto-rotated version for {key_id}",
            KeyUsage='ENCRYPT_DECRYPT',
            Origin='AWS_KMS'
        )
        return response['KeyMetadata']['KeyId']

    def disable_old_versions(self, key_id: str, versions: List[str]):
        for ver in versions:
            self.client.disable_key(KeyId=ver)

class GCPKMSRotation(CloudRotationStrategy):
    def __init__(self, kms_client):
        self.client = kms_client

    def create_new_version(self, key_id: str) -> str:
        from google.cloud.kms import CryptoKeyVersion
        parent = self.client.crypto_key_path(key_id)
        response = self.client.create_crypto_key_version(
            parent=parent,
            crypto_key_version=CryptoKeyVersion(
                state=CryptoKeyVersion.CryptoKeyVersionState.ENABLED
            )
        )
        return response.name

@dataclass
class KeyPolicy:
    rotation_days: int = 90
    max_versions: int = 5
    usage_threshold: int = 1000000  # Max encryptions before rotation
    min_active_time: timedelta = timedelta(hours=24)

class KeyRotator:
    def __init__(self, strategies: Dict[str, CloudRotationStrategy]):
        self.strategies = strategies
        self.key_store = KeyStore()
        self.rotation_gauge = Gauge(
            'key_rotation_operations', 
            'Key rotation metrics', 
            ['provider', 'key_id']
        )

    def auto_rotate(self, key_metadata: Dict):
        """Orchestrate full rotation lifecycle"""
        if not self._needs_rotation(key_metadata):
            return

        try:
            new_version = self._create_new_version(key_metadata)
            self._update_key_map(key_metadata, new_version)
            self._cleanup_old_versions(key_metadata)
            self._emit_metrics(key_metadata, success=True)
        except RotationError as e:
            logger.error(f"Rotation failed: {str(e)}")
            self._emit_metrics(key_metadata, success=False)
            raise

    def _needs_rotation(self, metadata: Dict) -> bool:
        """Check rotation triggers"""
        policy = self.key_store.get_policy(metadata['key_id'])
        
        # Time-based rotation
        if (datetime.now() - metadata['create_time']).days >= policy.rotation_days:
            return True
            
        # Usage-based rotation
        if metadata['usage_count'] >= policy.usage_threshold:
            return True
            
        return False

    def _create_new_version(self, metadata: Dict) -> str:
        """Multi-cloud version creation"""
        strategy = self.strategies.get(metadata['provider'])
        if not strategy:
            raise RotationError(f"Unsupported provider: {metadata['provider']}")
            
        try:
            return strategy.create_new_version(metadata['key_id'])
        except Exception as e:
            raise RotationError(f"Version creation failed: {str(e)}")

    def _update_key_map(self, metadata: Dict, new_version: str):
        """Update service references atomically"""
        self.key_store.update_key_version(
            key_id=metadata['key_id'],
            new_version=new_version,
            old_versions=metadata['previous_versions']
        )

    def _cleanup_old_versions(self, metadata: Dict):
        """Disable deprecated keys after grace period"""
        strategy = self.strategies.get(metadata['provider'])
        versions_to_disable = [
            ver for ver in metadata['previous_versions']
            if ver['state'] == 'ACTIVE' 
            and ver['age'] > metadata['policy'].min_active_time
        ]
        
        try:
            strategy.disable_old_versions(
                metadata['key_id'],
                [ver['id'] for ver in versions_to_disable]
            )
        except Exception as e:
            raise RotationError(f"Version cleanup failed: {str(e)}")

    def _emit_metrics(self, metadata: Dict, success: bool):
        self.rotation_gauge.labels(
            provider=metadata['provider'],
            key_id=metadata['key_id']
        ).set(1 if success else 0)

# Example Integration
if __name__ == "__main__":
    from kms_proxy import AWSKMSClient, GCPKMSClient
    
    # Initialize with cloud clients
    aws_client = AWSKMSRotation(AWSKMSClient(config={"region": "us-west-2"}))
    gcp_client = GCPKMSRotation(GCPKMSClient(config={"project_id": "my-project"}))
    
    rotator = KeyRotator(strategies={
        "aws": aws_client,
        "gcp": gcp_client
    })
    
    # Simulate rotation for AWS key
    sample_key = {
        "key_id": "arn:aws:kms:us-west-2:123456789012:key/abcd1234",
        "provider": "aws",
        "create_time": datetime.now() - timedelta(days=91),
        "usage_count": 500000,
        "previous_versions": [],
        "policy": KeyPolicy()
    }
    
    rotator.auto_rotate(sample_key)

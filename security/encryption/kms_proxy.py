# kms_proxy.py
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from cryptography.hazmat.backends import default_backend
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import kms

logger = logging.getLogger("KMS-Proxy")

class KMSError(Exception):
    """Base exception for KMS operations"""

class EncryptionError(KMSError):
    """Data encryption failure"""

class DecryptionError(KMSError):
    """Data decryption failure"""

class KeyRotationError(KMSError):
    """Key version rotation failure"""

@dataclass(frozen=True)
class KeyMetadata:
    provider: str
    key_id: str
    version: str
    creation_date: float

class BaseKMSClient(ABC):
    @abstractmethod
    def encrypt(self, plaintext: bytes, key_id: str) -> Tuple[bytes, Dict]:
        pass

    @abstractmethod
    def decrypt(self, ciphertext: bytes, key_id: str) -> bytes:
        pass

class AWSKMSClient(BaseKMSClient):
    def __init__(self, config: Dict):
        self.client = boto3.client(
            'kms',
            config=Config(
                retries={'max_attempts': 5, 'mode': 'standard'},
                region_name=config.get('region')
            )
        )

    def encrypt(self, plaintext: bytes, key_id: str) -> Tuple[bytes, Dict]:
        try:
            response = self.client.encrypt(
                KeyId=key_id,
                Plaintext=plaintext,
                EncryptionAlgorithm='SYMMETRIC_DEFAULT'
            )
            return response['CiphertextBlob'], {
                'KeyId': response['KeyId'],
                'EncryptionAlgorithm': response['EncryptionAlgorithm']
            }
        except ClientError as e:
            logger.error(f"AWS KMS encrypt failed: {str(e)}")
            raise EncryptionError from e

    def decrypt(self, ciphertext: bytes, key_id: str) -> bytes:
        try:
            response = self.client.decrypt(
                CiphertextBlob=ciphertext,
                KeyId=key_id,
                EncryptionAlgorithm='SYMMETRIC_DEFAULT'
            )
            return response['Plaintext']
        except ClientError as e:
            logger.error(f"AWS KMS decrypt failed: {str(e)}")
            raise DecryptionError from e

class GCPKMSClient(BaseKMSClient):
    def __init__(self, config: Dict):
        self.client = kms.KeyManagementServiceClient()
        self.project_id = config['project_id']
        self.location = config['location']

    def encrypt(self, plaintext: bytes, key_id: str) -> Tuple[bytes, Dict]:
        try:
            key_path = self.client.crypto_key_path_path(
                self.project_id,
                self.location,
                key_id.split('/')[3],  # key ring
                key_id.split('/')[5]    # crypto key
            )
            response = self.client.encrypt(
                name=key_path,
                plaintext=plaintext
            )
            return response.ciphertext, {
                'key_path': key_path,
                'protection_level': response.protection_level.name
            }
        except GoogleAPICallError as e:
            logger.error(f"GCP KMS encrypt failed: {str(e)}")
            raise EncryptionError from e

    def decrypt(self, ciphertext: bytes, key_id: str) -> bytes:
        try:
            key_path = self.client.crypto_key_path_path(
                self.project_id,
                self.location,
                key_id.split('/')[3],
                key_id.split('/')[5]
            )
            response = self.client.decrypt(
                name=key_path,
                ciphertext=ciphertext
            )
            return response.plaintext
        except GoogleAPICallError as e:
            logger.error(f"GCP KMS decrypt failed: {str(e)}")
            raise DecryptionError from e

class KMSProxy:
    def __init__(self, config: Dict):
        self.clients = {}
        self.key_map = config['key_map']
        
        if 'aws' in config:
            self.clients['aws'] = AWSKMSClient(config['aws'])
        if 'gcp' in config:
            self.clients['gcp'] = GCPKMSClient(config['gcp'])
        
        self.key_cache: Dict[str, KeyMetadata] = {}
        self._last_key_update = 0

    @lru_cache(maxsize=1024)
    def encrypt(self, plaintext: bytes, key_alias: str) -> Tuple[bytes, KeyMetadata]:
        """Multi-cloud encryption with automatic key selection"""
        self._refresh_key_map()
        
        key_info = self.key_map.get(key_alias)
        if not key_info:
            raise EncryptionError(f"Unknown key alias: {key_alias}")
        
        client = self.clients.get(key_info['provider'])
        if not client:
            raise EncryptionError(f"Unconfigured provider: {key_info['provider']}")
        
        try:
            ciphertext, metadata = client.encrypt(plaintext, key_info['key_id'])
            key_meta = KeyMetadata(
                provider=key_info['provider'],
                key_id=key_info['key_id'],
                version=metadata.get('version', 'latest'),
                creation_date=time.time()
            )
            self.key_cache[key_alias] = key_meta
            return ciphertext, key_meta
        except Exception as e:
            logger.error(f"Encryption failed for {key_alias}: {str(e)}")
            raise EncryptionError from e

    def decrypt(self, ciphertext: bytes, key_alias: str) -> bytes:
        """Multi-provider decryption with version handling"""
        self._refresh_key_map()
        
        key_meta = self.key_cache.get(key_alias)
        if not key_meta:
            raise DecryptionError(f"Key not found in cache: {key_alias}")
        
        client = self.clients.get(key_meta.provider)
        if not client:
            raise DecryptionError(f"Unavailable provider: {key_meta.provider}")
        
        try:
            return client.decrypt(ciphertext, key_meta.key_id)
        except Exception as e:
            logger.error(f"Decryption failed for {key_alias}: {str(e)}")
            raise DecryptionError from e

    def rotate_key(self, key_alias: str, new_key_id: str):
        """Zero-downtime key rotation"""
        if key_alias not in self.key_map:
            raise KeyRotationError(f"Unknown key alias: {key_alias}")
        
        old_key = self.key_map[key_alias]
        self.key_map[key_alias] = {
            **old_key,
            'key_id': new_key_id,
            'version': f"{old_key.get('version', 0) + 1}"
        }
        self._refresh_key_map(force=True)

    def _refresh_key_map(self, force: bool = False):
        """Refresh key versions periodically"""
        if force or (time.time() - self._last_key_update > 300):  # 5 min cache
            # Implement version check logic for each provider
            self._last_key_update = time.time()

# Example Configuration
CONFIG = {
    "aws": {
        "region": "us-west-2"
    },
    "gcp": {
        "project_id": "my-project",
        "location": "global"
    },
    "key_map": {
        "database-key": {
            "provider": "aws",
            "key_id": "arn:aws:kms:us-west-2:123456789012:key/abcd1234...",
            "version": "1"
        },
        "queue-key": {
            "provider": "gcp",
            "key_id": "projects/my-project/locations/global/keyRings/my-ring/cryptoKeys/my-key",
            "version": "2"
        }
    }
}

# Usage Example
if __name__ == "__main__":
    proxy = KMSProxy(CONFIG)
    
    # Encrypt data
    ciphertext, metadata = proxy.encrypt(b"Sensitive payload", "database-key")
    print(f"Encrypted using {metadata.provider} key {metadata.key_id}")
    
    # Decrypt data
    try:
        plaintext = proxy.decrypt(ciphertext, "database-key")
        print(f"Decrypted: {plaintext.decode()}")
    except DecryptionError as e:
        print(f"Decryption failed: {str(e)}")

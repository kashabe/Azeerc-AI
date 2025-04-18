# lambda_layer.py
import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

@dataclass
class LambdaConfig:
    env: str = os.getenv("ENV", "dev")
    secrets_arn: str = os.getenv("SECRETS_ARN")
    kms_key_id: str = os.getenv("KMS_KEY_ID")
    runtime_timeout: int = 900
    memory_size: int = 3008
    max_retries: int = 2
    layers: list = None

class LambdaHandler:
    def __init__(self, config: LambdaConfig):
        self.config = config
        self._init_clients()
        self._cache = {}
        self._cold_start = True

    def _init_clients(self):
        self.lambda_client = boto3.client(
            'lambda',
            config=Config(
                retries={
                    'max_attempts': self.config.max_retries,
                    'mode': 'standard'
                }
            )
        )
        self.ssm = boto3.client('ssm')
        self.kms = boto3.client('kms')

    def _cold_start_wrapper(self, func: Callable) -> Callable:
        def wrapper(event, context):
            if self._cold_start:
                self._warmup()
                self._cold_start = False
            return func(event, context)
        return wrapper

    def _warmup(self):
        """Pre-load critical resources"""
        self._load_secrets()
        self._init_connections()

    def _load_secrets(self):
        try:
            secret = self.ssm.get_parameter(
                Name=self.config.secrets_arn,
                WithDecryption=True
            )
            self._cache['secrets'] = json.loads(secret['Parameter']['Value'])
        except ClientError as e:
            logger.error(f"Secrets load failed: {e.response['Error']['Message']}")
            raise

    def _encrypt_payload(self, data: Dict) -> str:
        try:
            response = self.kms.encrypt(
                KeyId=self.config.kms_key_id,
                Plaintext=json.dumps(data).encode()
            )
            return base64.b64encode(response['CiphertextBlob']).decode()
        except ClientError as e:
            logger.error(f"Encryption failed: {e.response['Error']['Message']}")
            raise

    def _decrypt_payload(self, encrypted: str) -> Dict:
        try:
            decrypted = self.kms.decrypt(
                CiphertextBlob=base64.b64decode(encrypted)
            )
            return json.loads(decrypted['Plaintext'])
        except ClientError as e:
            logger.error(f"Decryption failed: {e.response['Error']['Message']}")
            raise

    def _invoke_with_retry(self, function_name: str, payload: Dict) -> Dict:
        encrypted_payload = self._encrypt_payload(payload)
        
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps({"encrypted": encrypted_payload}),
                    Qualifier='$LATEST'
                )
                return json.loads(response['Payload'].read())
            except self.lambda_client.exceptions.ResourceNotFoundException:
                logger.error(f"Function {function_name} not found")
                raise
            except Exception as e:
                if attempt == self.config.max_retries:
                    raise
                logger.warning(f"Retry {attempt+1} for {function_name}")
                time.sleep(2 ** attempt)

    def _connection_pool(self):
        """Reuse persistent connections"""
        if 'http' not in self._cache:
            self._cache['http'] = {
                'session': requests.Session(),
                'adapter': requests.adapters.HTTPAdapter(
                    pool_connections=10,
                    pool_maxsize=10,
                    max_retries=3
                )
            }
        return self._cache['http']

    def handler(self, func: Callable) -> Callable:
        @self._cold_start_wrapper
        def decorated(event, context):
            try:
                decrypted = self._decrypt_payload(event['encrypted'])
                result = func(decrypted, context)
                return {
                    'statusCode': 200,
                    'encrypted': self._encrypt_payload(result)
                }
            except Exception as e:
                logger.error(f"Handler error: {str(e)}")
                return {
                    'statusCode': 500,
                    'error': "Internal server error"
                }
        return decorated

    def async_invoke(self, function_name: str, payload: Dict) -> str:
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            return response['StatusCode']
        except ClientError as e:
            logger.error(f"Async invoke failed: {e.response['Error']['Message']}")
            raise

### Enterprise Features ###

1. **Security**
```python
# KMS encryption for all payloads
# IAM role with least privilege
# Parameter Store for secrets

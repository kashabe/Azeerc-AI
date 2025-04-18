# jwt_manager.py
import logging
import os
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional, Tuple

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from jose import JWTError, jwt
from pydantic import BaseModel, ValidationError

logger = logging.getLogger("JWTManager")

class Algorithm(str, Enum):
    RS256 = "RS256"
    EdDSA = "EdDSA"
    HS256 = "HS256"

class KeyRotationConfig(BaseModel):
    rotation_interval: int = 3600  # Seconds
    key_storage_path: str = "/var/secrets/jwt"
    max_previous_keys: int = 3

class JWTClaims(BaseModel):
    sub: str
    exp: datetime
    iat: datetime
    iss: str = "azeerc.ai"
    aud: str = "azeerc.ai"
    jti: Optional[str] = None
    scopes: Optional[str] = None

class JWTManager:
    def __init__(self, algorithm: Algorithm = Algorithm.RS256):
        self.algorithm = algorithm
        self.current_key: Tuple[str, str, datetime] = (None, None, None)
        self.previous_keys: Dict[str, Tuple[str, datetime]] = {}
        self.rotation_config = KeyRotationConfig()
        self._initialize_keys()
        self._start_key_rotation()

    def _initialize_keys(self):
        if self.algorithm in [Algorithm.RS256, Algorithm.EdDSA]:
            self._load_or_generate_asymmetric_keys()
        else:
            self._load_symmetric_key()

    def _load_or_generate_asymmetric_keys(self):
        try:
            priv_path = f"{self.rotation_config.key_storage_path}/current_private.pem"
            pub_path = f"{self.rotation_config.key_storage_path}/current_public.pem"
            
            if os.path.exists(priv_path):
                with open(priv_path, "rb") as f:
                    private_key = f.read()
                with open(pub_path, "rb") as f:
                    public_key = f.read()
            else:
                if self.algorithm == Algorithm.RS256:
                    private_key = rsa.generate_private_key(
                        public_exponent=65537,
                        key_size=4096,
                        backend=default_backend()
                    )
                    public_key = private_key.public_key()
                else:  # EdDSA
                    private_key = ed25519.Ed25519PrivateKey.generate()
                    public_key = private_key.public_key()

                private_pem = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                public_pem = public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                )
                
                os.makedirs(self.rotation_config.key_storage_path, exist_ok=True)
                with open(priv_path, "wb") as f:
                    f.write(private_pem)
                with open(pub_path, "wb") as f:
                    f.write(public_pem)

            self.current_key = (
                private_pem.decode(),
                public_pem.decode(),
                datetime.utcnow()
            )
            
        except Exception as e:
            logger.critical(f"Key initialization failed: {str(e)}")
            raise

    def _load_symmetric_key(self):
        key = os.getenv("JWT_SECRET_KEY")
        if not key or len(key) < 64:
            raise ValueError("HS256 requires 64+ character secret key")
        self.current_key = (key, key, datetime.utcnow())

    def _start_key_rotation(self):
        if self.algorithm == Algorithm.HS256:
            return  # Symmetric keys require manual rotation
        
        def rotation_task():
            while True:
                time.sleep(self.rotation_config.rotation_interval)
                self._rotate_keys()

        import threading
        rotation_thread = threading.Thread(target=rotation_task, daemon=True)
        rotation_thread.start()

    def _rotate_keys(self):
        logger.info("Initiating cryptographic key rotation")
        try:
            # Archive current key
            self.previous_keys[self.current_key[1]] = (
                self.current_key[0], 
                self.current_key[2]
            )
            
            # Generate new keys
            if self.algorithm == Algorithm.RS256:
                new_private = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=4096,
                    backend=default_backend()
                )
                new_public = new_private.public_key()
            else:  # EdDSA
                new_private = ed25519.Ed25519PrivateKey.generate()
                new_public = new_private.public_key()

            # Convert to PEM format
            private_pem = new_private.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ).decode()
            
            public_pem = new_public.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ).decode()

            # Update current key
            self.current_key = (private_pem, public_pem, datetime.utcnow())
            
            # Prune old keys
            if len(self.previous_keys) > self.rotation_config.max_previous_keys:
                oldest_key = min(self.previous_keys.items(), key=lambda x: x[1][1])
                del self.previous_keys[oldest_key[0]]
                
        except Exception as e:
            logger.error(f"Key rotation failed: {str(e)}")

    def create_token(
        self,
        subject: str,
        scopes: Optional[str] = None,
        expires_minutes: int = 30,
        custom_claims: Optional[Dict] = None
    ) -> str:
        try:
            now = datetime.utcnow()
            payload = {
                "sub": subject,
                "iat": now,
                "exp": now + timedelta(minutes=expires_minutes),
                "iss": "azeerc.ai",
                "aud": "azeerc.ai",
                "jti": os.urandom(16).hex(),
                "scopes": scopes or ""
            }
            if custom_claims:
                payload.update(custom_claims)

            signing_key = self.current_key[0]
            return jwt.encode(
                payload,
                signing_key,
                algorithm=self.algorithm.value
            )
        except Exception as e:
            logger.error(f"Token creation failed: {str(e)}")
            raise

    def validate_token(
        self,
        token: str,
        require_scopes: Optional[str] = None
    ) -> JWTClaims:
        try:
            # Try current key first
            public_key = self.current_key[1]
            payload = jwt.decode(
                token,
                public_key,
                algorithms=[self.algorithm.value],
                options={
                    "require_sub": True,
                    "require_exp": True,
                    "require_iat": True,
                    "verify_aud": True,
                    "verify_iss": True
                },
                audience="azeerc.ai",
                issuer="azeerc.ai"
            )
            
            # If validation failed, try previous keys
        except JWTError as e:
            for pub_key, (priv_key, _) in self.previous_keys.items():
                try:
                    payload = jwt.decode(
                        token,
                        pub_key,
                        algorithms=[self.algorithm.value],
                        options={"verify_signature": True}
                    )
                    break
                except JWTError:
                    continue
            else:
                logger.warning(f"Invalid token signature: {str(e)}")
                raise

        try:
            claims = JWTClaims(**payload)
        except ValidationError as e:
            logger.error(f"Invalid token claims: {str(e)}")
            raise

        if require_scopes:
            token_scopes = set(claims.scopes.split())
            required_scopes = set(require_scopes.split())
            if not required_scopes.issubset(token_scopes):
                logger.warning(f"Insufficient scopes: {claims.scopes}")
                raise PermissionError("Insufficient privileges")

        return claims

    def get_jwks(self) -> Dict:
        keys = []
        if self.algorithm in [Algorithm.RS256, Algorithm.EdDSA]:
            keys.append({
                "kty": "RSA" if self.algorithm == Algorithm.RS256 else "OKP",
                "alg": self.algorithm.value,
                "use": "sig",
                "kid": "current",
                **self._get_jwk_params(self.current_key[1])
            })
            for idx, (pub, (_, timestamp)) in enumerate(self.previous_keys.items()):
                keys.append({
                    "kty": "RSA" if self.algorithm == Algorithm.RS256 else "OKP",
                    "alg": self.algorithm.value,
                    "use": "sig",
                    "kid": f"prev_{idx}",
                    **self._get_jwk_params(pub)
                })
        return {"keys": keys}

    def _get_jwk_params(self, public_key: str) -> Dict:
        if self.algorithm == Algorithm.RS256:
            key = serialization.load_pem_public_key(
                public_key.encode(),
                backend=default_backend()
            )
            numbers = key.public_numbers()
            return {
                "n": numbers.n,
                "e": numbers.e,
            }
        else:  # EdDSA
            key = serialization.load_pem_public_key(
                public_key.encode(),
                backend=default_backend()
            )
            return {
                "x": key.public_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PublicFormat.Raw
                ).hex()
            }

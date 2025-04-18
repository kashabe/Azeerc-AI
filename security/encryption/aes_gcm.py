# aes_gcm.py
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger("AES-GCM")

class AESKeyStrength(Enum):
    AES_128 = 16
    AES_192 = 24
    AES_256 = 32

class EncryptionError(Exception):
    """Critical encryption failure exception"""

class DecryptionError(Exception):
    """Data decryption failed exception"""

@dataclass(frozen=True)
class EncryptedPayload:
    ciphertext: bytes
    tag: bytes
    nonce: bytes
    aad: Optional[bytes] = None

class AESGCM:
    def __init__(
        self,
        key_strength: AESKeyStrength = AESKeyStrength.AES_256,
        nonce_length: int = 12  # Recommended GCM nonce size
    ):
        if nonce_length < 12:
            raise ValueError("GCM nonce must be at least 12 bytes")
        self.nonce_length = nonce_length
        self.key_strength = key_strength
        self.backend = default_backend()

    @classmethod
    def generate_key(cls, strength: AESKeyStrength = AESKeyStrength.AES_256) -> bytes:
        """Generate cryptographically secure AES key"""
        return os.urandom(strength.value)

    def encrypt(
        self,
        plaintext: bytes,
        key: bytes,
        aad: Optional[bytes] = None,
        nonce: Optional[bytes] = None
    ) -> EncryptedPayload:
        """Encrypt with AES-GCM and return structured payload"""
        self._validate_key(key)
        
        try:
            nonce = nonce or os.urandom(self.nonce_length)
            cipher = Cipher(
                algorithms.AES(key),
                modes.GCM(nonce),
                backend=self.backend
            )
            encryptor = cipher.encryptor()
            
            if aad:
                encryptor.authenticate_additional_data(aad)
            
            ciphertext = encryptor.update(plaintext) + encryptor.finalize()
            return EncryptedPayload(
                ciphertext=ciphertext,
                tag=encryptor.tag,
                nonce=nonce,
                aad=aad
            )
        except Exception as e:
            logger.error(f"Encryption failed: {str(e)}")
            raise EncryptionError("Data encryption failure") from e

    def decrypt(
        self,
        payload: EncryptedPayload,
        key: bytes
    ) -> bytes:
        """Decrypt and validate AES-GCM payload"""
        self._validate_key(key)
        
        try:
            cipher = Cipher(
                algorithms.AES(key),
                modes.GCM(payload.nonce, payload.tag),
                backend=self.backend
            )
            decryptor = cipher.decryptor()
            
            if payload.aad:
                decryptor.authenticate_additional_data(payload.aad)
            
            plaintext = decryptor.update(payload.ciphertext) + decryptor.finalize()
            return plaintext
        except InvalidTag as e:
            logger.warning("Decryption failed: Authentication tag mismatch")
            raise DecryptionError("Data tampering detected") from e
        except Exception as e:
            logger.error(f"Decryption error: {str(e)}")
            raise DecryptionError("Data decryption failure") from e

    def _validate_key(self, key: bytes):
        if len(key) != self.key_strength.value:
            raise ValueError(
                f"Invalid key length: Expected {self.key_strength.value} bytes, "
                f"got {len(key)}"
            )

    @staticmethod
    def generate_nonce(length: int = 12) -> bytes:
        """Generate recommended nonce for GCM mode"""
        return os.urandom(length)

    @staticmethod
    def derive_key_from_secret(
        secret: bytes,
        salt: bytes,
        iterations: int = 600000,
        key_length: int = 32
    ) -> bytes:
        """PBKDF2-HMAC-SHA256 key derivation"""
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.primitives.hashes import SHA256
        
        kdf = PBKDF2HMAC(
            algorithm=SHA256(),
            length=key_length,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        return kdf.derive(secret)

# Example usage
if __name__ == "__main__":
    # Initialize with AES-256
    crypto = AESGCM()
    
    # Generate encryption key
    key = AESGCM.generate_key()
    
    # Encrypt data
    payload = crypto.encrypt(
        plaintext=b"Sensitive enterprise data",
        key=key,
        aad=b"metadata"
    )
    
    # Decrypt data
    try:
        decrypted = crypto.decrypt(payload, key)
        print(f"Decrypted: {decrypted.decode()}")
    except DecryptionError as e:
        print(f"Security alert: {str(e)}")

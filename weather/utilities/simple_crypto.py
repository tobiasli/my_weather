"""Simple crypto is a thin class for encrypting and decrypting strings in a cryptographically secure manner."""
import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class SimpleCryptoEngine:
    """SimpleCryptoEngine is a small class for encrypting and decrypting strings with a password and salt in a
    cryptographically secure manner."""

    def __init__(self, password: str, salt: str) -> None:
        """SimpleCryptoEngine is a small class for encrypting and decrypting strings with a password and salt in a
        cryptographically secure manner.

        Uses PBKDF2HMAC for password stretching with 100000 iterations.

        Args:
            password: The password used to encrypt strings.
            salt: An additional, arbitrary string that should be specific for this specific password for this
                specific application (in the case that the password is not unique for this application).
        """
        self._engine = Fernet(self.generate_key(password, salt))

    @staticmethod
    def generate_key(password: str, salt: str) -> bytes:
        """Create a URL safe 32 byte key for encryption and decryption of sensitive environment variables."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt.encode(),
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))

        return key

    def encrypt(self, string: str) -> str:
        return self._engine.encrypt(string.encode()).decode()

    def decrypt(self, string: str) -> str:
        return self._engine.decrypt(string.encode()).decode()
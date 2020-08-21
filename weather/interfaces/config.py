"""Classes and Metaclasses used for config handling in my_weather."""
import os

import typing as ty

from abc import abstractmethod
from collections.abc import Mapping
from cryptography.fernet import InvalidToken

import tregex

from weather.utilities.simple_crypto import SimpleCryptoEngine


class RepositoryConfigError(Exception):
    """Errors raised by the repo config."""
    pass


class EncryptedEnvVarError(Exception):
    """Errors raised by the EncryptedEnvVarConfig"""
    pass


class RepositoryConfigBase(Mapping):
    """Unpackable (**) Container for repository configuration arguments."""

    def __new__(cls, *args, **kwargs):
        for unpack_prop in cls._unpack_props:
            if unpack_prop not in cls.__dict__:
                raise RepositoryConfigError(f'Class {cls.__name__} has a bad unpack property {unpack_prop}.')
        return super(RepositoryConfigBase, cls).__new__(cls)

    @property
    @abstractmethod
    def _unpack_props(self) -> ty.List[str]:
        """A list containing all properties we want to unpack with **."""

    def __iter__(self):
        for key in self._unpack_props:
            yield key

    def __len__(self):
        return len(self._unpack_props)

    def __getitem__(self, item):
        return getattr(self, item)


class EnvVarConfig:
    """Superclass containing methods for verifying and getting environment variables."""

    @staticmethod
    def verify_env_var(var: str) -> str:
        """Simple check if variable exists in environment."""
        if var not in os.environ:
            raise EnvironmentError(f"Can't find environment variable {var}. "
                                   f"Closest match is {tregex.find_best(var, [var for var in os.environ])}.")
        return var

    @staticmethod
    def get_env_var(var: str) -> str:
        """Get the value from a named environment variable."""
        return os.environ.get(var, None)


class EncryptedEnvVarConfig(EnvVarConfig):
    """Superclass containing methods for verifying and getting sha-512 encrypted environment variables."""

    def __init__(self, password: str, salt: str) -> None:
        """Password encrypted environment variables"""
        self.engine = SimpleCryptoEngine(password, salt)

    def get_env_var(self, var: str) -> str:
        """Get the value from an encrypted, named environment variable."""
        try:
            return self.engine.decrypt(os.environ.get(var, None))
        except InvalidToken:
            raise EncryptedEnvVarError(f'Cannot decrypt environment variable {var} with key {os.environ.get(var, None)}')

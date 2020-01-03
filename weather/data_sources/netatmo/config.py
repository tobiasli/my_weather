"""Config classes and definitions used for the Netatmo repository."""
import typing as ty
from abc import ABC, abstractmethod

from weather.interfaces.config import RepositoryConfigBase, EnvVarConfig


class NetatmoConfigBase(ABC, RepositoryConfigBase):
    """Metaclass defining the properties that are expected from a NetatmoConfig."""

    _unpack_props = ['username', 'password', 'client_id', 'client_secret', 'api_limits', 'direct_login']

    @property
    @abstractmethod
    def username(self) -> str:
        """This property must contain the username for Netatmo."""

    @property
    @abstractmethod
    def password(self) -> str:
        """This property must contain the password for Netatmo."""

    @property
    @abstractmethod
    def client_id(self) -> str:
        """This property must contain the client_id for Netatmo."""

    @property
    @abstractmethod
    def client_secret(self) -> str:
        """This property must contain the client_secret for Netatmo."""

    @property
    @abstractmethod
    def api_limits(self) -> ty.Dict[str, ty.Dict[str, int]]:
        """This property must contain the optional api_limits for Netatmo."""

    @property
    @abstractmethod
    def direct_login(self) -> bool:
        """This property must contain the optional direct_login for Netatmo."""


class NetatmoEnvironmentVariablesConfig(NetatmoConfigBase, EnvVarConfig):
    """Netatmo config information fetched partially from environment_variables."""
    def __init__(self,
                 username_var: str,
                 password_var: str,
                 client_id_var: str,
                 client_secret_var: str,
                 api_limits: ty.Dict[str, ty.Dict[str, int]] = None,
                 direct_login: bool = True
                 ) -> None:
        self.username_var = self.verify_env_var(username_var)
        self.password_var = self.verify_env_var(password_var)
        self.client_id_var = self.verify_env_var(client_id_var)
        self.client_secret_var = self.verify_env_var(client_secret_var)

        self._direct_login = direct_login
        self._api_limits = api_limits or {}

    @property
    def username(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.username_var)

    @property
    def password(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.password_var)

    @property
    def client_id(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.client_id_var)

    @property
    def client_secret(self) -> str:
        """Return the client_secret for the Netatmo instance."""
        return self.get_env_var(self.client_secret_var)

    @property
    def direct_login(self) -> bool:
        """Return the choice of direct login: If we initiate the Netatmo API on construction or if we wait."""
        return self._direct_login

    @property
    def api_limits(self) -> ty.Dict[str, ty.Dict[str, int]]:
        """Return the api limits for the RateLimiter for the Netatmo instance."""
        return self._api_limits

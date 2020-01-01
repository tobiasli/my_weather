"""Test interfaces abstractclasses."""
import pytest
import os
import typing as ty
from abc import ABC, abstractmethod

from weather.interfaces.config import RepositoryConfigBase, RepositoryConfigError, EnvironmentVariablesConfigBase


def test_repository_config_good_construction():

    class TestConfigBase(ABC, RepositoryConfigBase):
        _unpack_props = ['arg1', 'arg2', 'arg3']

        @property
        @abstractmethod
        def arg1(self) -> str:
            """Return argument 1."""

        @property
        @abstractmethod
        def arg2(self) -> int:
            """Return argument 2."""

        @property
        @abstractmethod
        def arg3(self) -> ty.List[int]:
            """Return argument 3."""
            
    class TestConfig(TestConfigBase):
        def __init__(self, arg1: str, arg2: int, arg3: ty.List[int]):
            self._arg1 = arg1
            self._arg2 = arg2
            self._arg3 = arg3

        @property
        def arg1(self):
            return self._arg1

        @property
        def arg2(self):
            return self._arg2

        @property
        def arg3(self):
            return self._arg3

    assert TestConfig(arg1='something', arg2=2, arg3=[1, 2, 3])


def test_repository_config_unpack():

    class TestConfigBase(ABC, RepositoryConfigBase):
        _unpack_props = ['arg1', 'arg2', 'arg3']

        @property
        @abstractmethod
        def arg1(self) -> str:
            """Return argument 1."""

        @property
        @abstractmethod
        def arg2(self) -> int:
            """Return argument 2."""

        @property
        @abstractmethod
        def arg3(self) -> ty.List[int]:
            """Return argument 3."""

    class TestConfig(TestConfigBase):
        def __init__(self, arg1: str, arg2: int, arg3: ty.List[int]):
            self._arg1 = arg1
            self._arg2 = arg2
            self._arg3 = arg3

        @property
        def arg1(self):
            return self._arg1

        @property
        def arg2(self):
            return self._arg2

        @property
        def arg3(self):
            return self._arg3

    config = TestConfig(arg1='something', arg2=2, arg3=[1, 2, 3])

    assert dict(**config) == dict(arg1='something', arg2=2, arg3=[1, 2, 3])


def test_repository_config_incomplete_subclass_methods():
    class TestConfigBase(ABC, RepositoryConfigBase):
        _unpack_props = ['arg1', 'arg2', 'arg3']

        @property
        @abstractmethod
        def arg1(self) -> str:
            """Return argument 1."""

        @property
        @abstractmethod
        def arg2(self) -> int:
            """Return argument 2."""

        @property
        @abstractmethod
        def arg3(self) -> ty.List[int]:
            """Return argument 3."""

    class TestConfigBad(TestConfigBase):
        def __init__(self, arg1: str, arg2: int, arg3: ty.List[int]):
            self._arg1 = arg1
            self._arg2 = arg2
            self._arg3 = arg3

        @property
        def arg1(self):
            return self._arg1

        @property
        def arg2(self):
            return self._arg2

    with pytest.raises(RepositoryConfigError):
        assert TestConfigBad(arg1='something', arg2=2, arg3=[1, 2, 3])


def test_environment_config():
    class TestConfigBase(ABC, RepositoryConfigBase):
        _unpack_props = ['arg1', 'arg2', 'arg3']

        @property
        @abstractmethod
        def arg1(self) -> str:
            """Return argument 1."""

        @property
        @abstractmethod
        def arg2(self) -> int:
            """Return argument 2."""

        @property
        @abstractmethod
        def arg3(self) -> ty.List[int]:
            """Return argument 3."""

    class TestConfig(TestConfigBase, EnvironmentVariablesConfigBase):
        def __init__(self, arg1_var: str, arg2_var: str, arg3_var: str):
            self.arg1_var = self.verify_env_var(arg1_var)
            self.arg2_var = self.verify_env_var(arg2_var)
            self.arg3_var = self.verify_env_var(arg3_var)

        @property
        def arg1(self):
            return self.get_env_var(self.arg1_var)

        @property
        def arg2(self):
            return int(self.get_env_var(self.arg2_var))

        @property
        def arg3(self):
            return [int(num) for num in self.get_env_var(self.arg3_var).split(',')]

    # Set env variables:
    os.environ['arg1_var'] = 'something'
    os.environ['arg2_var'] = '2'
    os.environ['arg3_var'] = '1,2,3'

    config = TestConfig(arg1_var='arg1_var', arg2_var='arg2_var', arg3_var='arg3_var')

    assert dict(**config) == dict(arg1='something', arg2=2, arg3=[1, 2, 3])

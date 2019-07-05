"""Tools for managing various services."""

import logging
import threading
import typing as ty
from abc import abstractmethod, ABC

Number = ty.Union[float, int]


class ServiceBaseClass(ABC):
    """Base class for managed services."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the service, so it is identifyable in logs."""

    @abstractmethod
    def start(self) -> None:
        """Starting the service."""

    @abstractmethod
    def stop(self) -> None:
        """Stopping the service."""

    @abstractmethod
    def service_callback(self) -> None:
        """Service callback called asynchronously by the ServiceManager."""


class ServiceLoop:
    """Service loop is a container for services that are called by a Thread."""

    def __init__(self, service_instance: ServiceBaseClass) -> None:
        """ServiceLoops are used internally in ServiceManagers so they can perform asynchronous calls towards the
        services.

        Args:
            service_instance: The service that
        """
        self.service_instance = service_instance

    def _log_msg(self, msg: str) -> str:
        """Format a logging message."""
        return f'{str(type(self.service_instance))}[{self.service_instance.name}]: {msg}'

    def __call__(self) -> None:
        """Target callback called by the Thread."""
        t = threading.currentThread()
        # Loop when continue_loop==True.
        # In the first pass, continue loop might not have been set yet, so we assume True.
        logging.info(self._log_msg('Async start.'))
        while getattr(t, "continue_loop", True):
            self.service_instance.service_callback()

        logging.info(self._log_msg('Async stop.'))


class ServiceManager:
    """Class for managing a single service."""

    def __init__(self, service: ServiceBaseClass, heartbeat_method: ty.Callable[[], bool]) -> None:
        self.service = service
        self.heartbeat_method = heartbeat_method
        self.thread: threading.Thread = None

    def check_health(self) -> bool:
        """Simplest possible request that could verify the status of the service."""
        if self.thread:
            return self.heartbeat_method() and self.thread.is_alive()

    def start(self) -> None:
        """Method that starts the service."""
        self.thread = threading.Thread(name=self.service.name,
                                       target=ServiceLoop(service_instance=self.service))
        self.thread.continue_loop = True
        self.thread.start()

    def stop(self) -> None:
        """Method that stops the service."""
        if isinstance(self.thread, threading.Thread):
            self.thread.continue_loop = False


class ServiceBoss:
    """Class for managing a set of services."""

    def __init__(self, services: ty.Sequence[ServiceManager], health_check_frequency: Number) -> None:
        """Initiate a ServiceManager with a set of services to maintain.

        Args:
            services: The ServiceBaseClass classes to manage.
            health_check_frequency: Number of seconds between each health check.
        """
        self.services = services
        self.frequency = health_check_frequency

    def start_services(self) -> None:
        """Start all managed services."""
        for service in self.services:
            service.start()

        thread = threading.Thread(name='service_boss',
                                  target=ServiceLoop())

    def stop_services(self) -> None:
        """Stop all managed services."""
        for service in self.services:
            service.stop()
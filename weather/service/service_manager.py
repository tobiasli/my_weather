"""Tools for managing various services.

If you have a task you want to perform asynchronously, subclass ServiceTask and add a .perform() method for the task.

This task is added as a ServiceTask is added to a Service with a heartbeat. The heartbeat is a callable that is capable
verifying if the ServiceTask loop is performing properly or not. If it is not performing, it will be restarted.
"""

import logging
import time
import threading
import typing as ty

Number = ty.Union[float, int]


class ServiceError(Exception):
    """Exception raised by a service."""


class ServiceLoop:
    """Service loop is a container for services that are called by a Thread."""

    def __init__(self, name: str, task: ty.Callable, task_delay) -> None:
        """ServiceLoops are used internally in ServiceManagers so they can perform asynchronous calls towards the
        services.

        Args:
            name: Name of the task. Used for logging.
            task: The ServiceTask that should be performed asynchronously.
            task_delay: The delay in seconds between each task execution.
        """
        self.name = name
        self.task = task
        self.task_delay = task_delay

    def __call__(self) -> None:
        """Target callback called by the Thread."""
        t = threading.currentThread()
        # Loop when continue_loop==True.
        # In the first pass, continue loop might not have been set yet, so we assume True.
        logging.info('Service start.')
        while getattr(t, "continue_loop", True):
            self.task()
            time.sleep(self.task_delay)

        logging.info('Service stop.')


class Service:
    """Class for managing a single service."""

    def __init__(self,
                 name: str,
                 task: ty.Callable,
                 task_delay: Number,
                 health_check: ty.Callable[[], bool] = None,
                 restart_action: ty.Callable = None
                 ) -> None:
        self.name = name
        self.task = task
        self.task_delay = task_delay
        self.health_check = health_check if health_check else lambda: True
        self.restart_action = restart_action
        self.thread: threading.Thread = None

        if not self.task_delay > 0:
            raise ServiceError('task_delay/frequency must be higher than 0 seconds.')

    def healthy(self) -> bool:
        """Verify that the service is alive, and that the health_check callable returns True."""
        return self.health_check() and self.thread.is_alive() if self.thread else False

    def start(self) -> None:
        """Method that starts the service."""
        self.thread = threading.Thread(name=self.name,
                                       target=ServiceLoop(name=self.name, task=self.task, task_delay=self.task_delay))
        self.thread.continue_loop = True
        self.thread.start()

    def stop(self) -> None:
        """Method that stops the service."""
        logging.info(f'Stopping service {self.name}')
        if isinstance(self.thread, threading.Thread):
            self.thread.continue_loop = False

    def restart(self) -> None:
        """Perform a restart operation that might help if we can't perform task (healthy == False)"""
        self.stop()
        if self.restart_action:
            self.restart_action()
        self.start()


class Maintainer:
    """Class for maintaining the health of a set of services."""
    name = 'maintainer'

    def __init__(self, managed_services: ty.Sequence[Service]) -> None:
        """Take a set of services and maintain their health by restarting them if they are not healthy or not alive."""
        self.managed_services = managed_services

    def perform(self) -> None:
        """Check if services are healthy and restart if they are not."""


class ServiceManager:
    """Class for managing a set of services."""

    def __init__(self, services: ty.Sequence[Service], health_check_frequency: Number) -> None:
        """Initiate a ServiceManager with a set of services to maintain.

        Args:
            services: The ServiceBaseClass classes to manage.
            health_check_frequency: Number of seconds between each health check.
        """
        self.services = services
        self.maintainer = Service(
            name='maintainer',
            task=self.check_service_health_and_restart,
            task_delay=health_check_frequency
        )

    def check_service_health_and_restart(self) -> None:
        """Check the health of all services and restart if necessary."""
        for service in self.services:
            if not service.healthy():
                service.restart()

    def start_services(self) -> None:
        """Start all managed services."""
        for service in self.services:
            service.start()
        self.maintainer.start()

    def stop_services(self) -> None:
        """Stop all managed services."""
        self.maintainer.stop()
        for service in self.services:
            service.stop()

    def restart_services(self) -> None:
        """Restart all services."""
        self.maintainer.stop()
        for service in self.services:
            service.restart()
        self.maintainer.start()


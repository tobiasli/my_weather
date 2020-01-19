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
    """Service loop is a callable container for a task that is called by threading.Thread."""

    def __init__(self, name: str,
                 task: ty.Callable[[], None] = None,
                 task_delay: Number = None) -> None:
        """ServiceLoops are used internally in Services so they can perform asynchronous calls towards the
        tasks.

        Args:
            name: Name of the task. Used for logging.
            task: The callable that is performed asynchronously.
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
        logging.info(f'Service {self.name} started.')
        while getattr(t, "continue_loop", True):
            if self.task:
                self.task()
            time.sleep(self.task_delay)

        logging.info(f'Service {self.name} stopped.')


class Service:
    """Class for managing a single task."""

    def __init__(self,
                 name: str,
                 task: ty.Optional[ty.Callable[[], None]] = None,
                 task_interval: ty.Optional[Number] = None,
                 health_check_action: ty.Optional[ty.Callable[[], bool]] = None,
                 restart_action: ty.Optional[ty.Callable[[], None]] = None
                 ) -> None:
        """A Service is a class for managing a task that we want to perform regularly. It can also monitor the health of
        the task via a callable, and perform a restart_action if the health_check returns False.

        Notes:
            The Service does not need a task. It can be a container for a health_check and a restart_action for
        another service that needs monitoring.
            If the Service is not provided with a task, it does not fire up a Thread and a ServiceLoop. healthy() only
        checks the

        Args:
            name: Name of the service used as the thread name.
            task: Callable (method) that we want to perform regularly.
            task_interval: Number of seconds between each task execution.
            health_check_action: Callable (method) that verifies health of task/service. True for healthy, False for
                unhealthy.
            restart_action: Callable (method) performed when health_check == False to try to fix the task/service.
        """

        if task and task_interval <= 0:
            raise ServiceError('task_delay/frequency must be higher than 0 seconds.')

        self.name = name
        self.task = task
        self.task_interval = task_interval
        self.health_check_action = health_check_action if health_check_action else lambda: True
        self.restart_action = restart_action

        self.thread: threading.Thread = None

    def healthy(self) -> bool:
        """Verify that health_check_action returns True and if we have a task check that the thread is alive."""
        if self.task:
            service_health = self.thread.is_alive() if self.thread else False
        else:
            service_health = True
        return self.health_check_action() and service_health

    def start(self) -> None:
        """Method that starts the service."""
        if self.task:
            self.thread = threading.Thread(name=self.name,
                                           target=ServiceLoop(name=self.name,
                                                              task=self.task,
                                                              task_delay=self.task_interval))
            self.thread.continue_loop = True
            self.thread.start()

    def stop(self) -> None:
        """Method that stops the service."""
        if self.task:
            logging.info(f'Stopping service {self.name}')
            if isinstance(self.thread, threading.Thread):
                self.thread.continue_loop = False

    def restart(self) -> None:
        """Perform a restart operation on Service. Usually when self.healthy() == False."""
        self.stop()
        if self.restart_action:
            self.restart_action()
        self.start()


class MaintainerService(Service):
    """A MaintainerService acts like a regular service, but has a dynamic name property that contains information
    regarding the services that the MaintainerService is set to maintain."""

    # noinspection PyMissingConstructor
    def __init__(self,
                 service_manager: "ServiceManager",
                 task: ty.Optional[ty.Callable[[], None]] = None,
                 task_interval: ty.Optional[Number] = None,
                 health_check_action: ty.Optional[ty.Callable[[], bool]] = None,
                 restart_action: ty.Optional[ty.Callable[[], None]] = None
                 ) -> None:
        """A MaintainerService acts like a regular service but is aware of it's own ServiceManager. We want the name of
        the MaintainerService to reflect the services that it maintains. As this list is mutable, being aware of the
        ServiceManager let's it query the services names any time it is necessary.

        Args:
            service_manager: The ServiceManager handling this MaintainerService.
            task: Callable (method) that we want to perform regularly.
            task_interval: Number of seconds between each task execution.
            health_check_action: Callable (method) that verifies health of task/service. True for healthy, False for
                unhealthy.
            restart_action: Callable (method) performed when health_check == False to try to fix the task/service.
        """

        if task and task_interval <= 0:
            raise ServiceError('task_delay/frequency must be higher than 0 seconds.')

        self.task = task
        self.task_interval = task_interval
        self.health_check_action = health_check_action if health_check_action else lambda: True
        self.restart_action = restart_action

        self.thread: threading.Thread = None
        self.service_manager = service_manager

    @property
    def name(self) -> str:
        """The maintainer name contains a reference to the underlying services that are maintained. Since this list
        of services is mutable, the maintainer_name needs to be dynamic."""
        return f'maintainer[{", ".join([service.name for service in self.service_manager.services])}]'


class ServiceManager:
    """Class for managing a set of services."""

    def __init__(self, services: ty.Sequence[Service] = None, health_check_interval: Number = 60) -> None:
        """Initiate a ServiceManager with a set of services to maintain.

        Args:
            services: The ServiceBaseClass classes to manage.
            health_check_interval: Number of seconds between each health check. Defaults to 60 seconds.
        """
        self.services = services if services else list()
        self.maintainer = MaintainerService(
            service_manager=self,
            task=self.check_service_health_and_restart,
            task_interval=health_check_interval
        )

    def add_service(self, service: Service) -> None:
        """Add service to managed services."""
        self.services.append(service)
        logging.info(f'Service {service.name} added to {self.maintainer.name}')

    def check_service_health_and_restart(self) -> None:
        """Check the health of all services and restart if necessary."""
        for service in self.services:
            if not service.healthy():
                logging.info(f'{self.maintainer.name} is restarting service {service.name}')
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

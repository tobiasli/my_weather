"""Test components related to the ServiceManager."""
import time
import logging

from weather.service.service_manager import ServiceManager, Service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])


class TestTask:
    """Dummy task for test."""
    state = 1
    value = 0

    def __init__(self, name):
        self.name = name

    def perform(self):
        logging.info(f'{self.name} is performed.')
        self.value = 1

    def health_check(self):
        logging.info(f'{self.name} is checked.')
        return bool(self.state)

    def restart(self):
        logging.info(f'{self.name} is restarted.')
        self.state = 1

    def set_state(self, state):
        self.state = state
        logging.info(f'{self.name} state set to is {state}')


def test_service_manager():
    """Check the health of a service.
    Manipulate the state and verify that state is not ok. Restart service and check
    that state is repaired."""
    task = TestTask('test_task')
    sm = Service(name=task.name, task=task.perform, task_interval=0.1, health_check_action=task.health_check)
    assert not sm.healthy()  # Task not started yet.
    sm.start()
    try:
        assert sm.healthy()
        task.set_state(0)
        assert not sm.healthy()  # Heartbeat returns False.
        assert task.value  # Tasks have actually been performed.
    finally:
        sm.stop()


def test_service_boss():
    task1 = TestTask('test_1')
    serv1 = Service(name=task1.name, task=task1.perform, task_interval=1, health_check_action=task1.health_check, restart_action=task1.restart)
    task2 = TestTask('test_2')
    serv2 = Service(name=task2.name, task=task2.perform, task_interval=1, health_check_action=task2.health_check, restart_action=task2.restart)

    sm = ServiceManager(services=[serv1], health_check_frequency=0.02)
    sm.add_service(serv2)
    sm.start_services()
    try:
        task2.set_state(0)
        time.sleep(0.1)
        assert task2.state == 1  # State has been automatically set to 1 via health_check and restart.
    finally:
        sm.stop_services()
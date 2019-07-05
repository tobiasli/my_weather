"""Test components related to the ServiceManager."""
import time
from weather.service.service_manager import ServiceBaseClass, ServiceBoss, ServiceManager


class TestService(ServiceBaseClass):
    """Dummy service for test."""
    name = 'test_service'
    value = 0

    def start(self):
        self.value = 1

    def stop(self):
        self.value = 0

    def service_callback(self):
        print(f'Running service: value={self.value}')
        time.sleep(0.0001)


def test_service_manager():
    """Check the health of a service. Manipulate the state and verify that state is not ok."""
    service = TestService()

    sm = ServiceManager(service=service, heartbeat_method=lambda: bool(service.value))

    sm.start()

    sm.check_health()

    service.value = 0

    assert not sm.check_health()

    sm.stop()


def test_service_boss():

    service = TestService()
    service.name = 'test_1'
    sm1 = ServiceManager(service=service, heartbeat_method=lambda: bool(service.value))
    service = TestService()
    service.name = 'test_2'
    sm2 = ServiceManager(service=service, heartbeat_method=lambda: bool(service.value))

    boss = ServiceBoss(services=[sm1, sm2], health_check_frequency=0.0001)
    boss.start_services()
    time.sleep(0.01)
    boss.services[0].service.value = 0
    time.sleep(0.01)
    for service in boss.services:
        assert service.service.value
    boss.stop_services()
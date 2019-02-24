from utilities.rate_limiter import RateLimiter, utctime_now
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])


def test_current_no_actions():
    limiter = RateLimiter(action_limit=10, timespan=10)
    assert limiter.check_current_rate()  # No calls performed, should pass.


def test_next_no_actions():
    limiter = RateLimiter(action_limit=10, timespan=10)
    assert limiter.check_next_rate()  # No calls performed, should pass.


def test_next_slighlty_few_actions():
    limiter = RateLimiter(action_limit=10, timespan=10)
    for i in range(9):
        limiter.perform_action()
    assert limiter.check_next_rate()


def test_next_slightly_many_actions():
    limiter = RateLimiter(action_limit=10, timespan=10)
    for i in range(10):
        limiter.perform_action()
    assert not limiter.check_next_rate()


def test_decorate_function():
    limiter = RateLimiter(action_limit=10, timespan=0.5, wait_time=0.1)

    @limiter.rate_limit_decorator
    def times_two(number):
        return number*2

    start = utctime_now()
    for i in range(10):
        times_two(i)
    end = utctime_now()
    assert end-start < limiter.wait_time  # The limit should not have been met, and the call should go fast.
    times_two(11)
    end = utctime_now()
    assert end-start > limiter.wait_time  # That last call should have tripped the limiter.


def test_double_decorate_function():
    limiter1 = RateLimiter(action_limit=3, timespan=0.5, wait_time=0.1)
    limiter2 = RateLimiter(action_limit=5, timespan=2, wait_time=0.2)

    @limiter1.rate_limit_decorator
    @limiter2.rate_limit_decorator
    def times_two(number):
        return number*2

    start = utctime_now()
    for i in range(3):
        times_two(i)
    end = utctime_now()
    assert end-start < limiter1.wait_time  # The limit should not have been met, and the call should go fast.

    start = utctime_now()
    times_two(4)
    end = utctime_now()
    assert end-start > limiter1.wait_time  # limiter1 is tripped.

    start = utctime_now()
    times_two(5)
    end = utctime_now()
    assert end-start < limiter1.wait_time  # limiter1 noe longer tripped.

    start = utctime_now()
    times_two(6)
    times_two(7)
    times_two(8)
    times_two(9)
    times_two(10)
    times_two(11)
    end = utctime_now()
    assert end-start > limiter1.wait_time + limiter2.wait_time  # both should have been tripped in action streak.

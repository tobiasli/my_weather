from shyft.api import utctime_now, time
from typing import Union, Sequence, Callable
from collections import deque
from time import sleep
import logging

TimeType = Union[float, int, time]


class RateLimiter:
    def __init__(self, *, action_limit: int, timespan: TimeType, wait_time: TimeType = 1) -> None:
        """This class defines a rate limiter, that according to a set of calls, timespans and wait times, assures that
        actions do not exceed a given rate.

        Args:
            action_limit: The maximum amount of allowed actions within timespan.
            timespan: The timespan in seconds over which the number of actions in action_limit is allowed.
            wait_time: De set amount of wait time when the limit is reached.
        """
        self.action_limit = action_limit
        self.timespan = timespan
        self.wait_time = wait_time
        self.action_timestamps = deque(maxlen=action_limit)

    @staticmethod
    def _check_rate(*, action_timestamps: Sequence[TimeType], action_limit: int, timespan: TimeType) -> bool:
        """With a set of timestamps, an action limit and a timespan, check if the performed actions exceed the rate
        limit or not.

        Args:
            action_timestamps: The history of actions represented by their timestamps.
            action_limit: The maximum amount of allowed actions within timestamp.
            timespan: The timespan over which action_limit is allowed.

        Returns:
            True when within rate, False, when rate is exceeded.
        """

        if len(action_timestamps) < action_limit:
            return True

        now = utctime_now()
        # Check if the Nth call happened less than a timespan ago:
        if now - action_timestamps[-action_limit] < timespan:
            return False
        else:
            return True

    def check_current_rate(self) -> bool:
        """Check if the actions that have been performed exceeded the rate limit."""

        return self._check_rate(action_timestamps=self.action_timestamps,
                                action_limit=self.action_limit,
                                timespan=self.timespan)

    def check_next_rate(self) -> bool:
        """Check if a hypothetical next action (performed right now) would exceed the rate limit."""

        now = utctime_now()
        next_timestamps = list(self.action_timestamps)[1:]
        next_timestamps.append(now)
        return self._check_rate(action_timestamps=next_timestamps,
                                action_limit=self.action_limit,
                                timespan=self.timespan)

    def perform_action(self) -> None:
        """Add now timestamp indicating an action to historical action timestamps."""
        self.action_timestamps.append(utctime_now())

    def add_action_timestamp(self, timestamp: TimeType) -> None:
        """Add an action timestamp to the list of historical action timestamps."""
        self.action_timestamps.append(timestamp)

    def check_next_and_wait(self, method_name: str = '') -> None:
        """Check the potential rate for next call, and wait if we are about to breach the limit."""

        while not self.check_next_rate():
            logging.info(f'{RateLimiter.__name__}: {method_name} asleep for {self.wait_time}s. Limit: {self.action_limit} actions pr {self.timespan}s')
            sleep(self.wait_time)

    def rate_limit_decorator(self, func) -> Callable:
        """Decorator function that can wrap other functions with rate limiting functionality, so that noe API limits
        are tripped when performing regular calls.

        Args:
            func: The method that is wrapped with a rate limiter.
        """

        def rate_limited_func(*args, **kwargs):
            """Function with a rate limiter that can not be called with a higher frequency than defined by the
            RateLimiter class."""
            self.check_next_and_wait(func.__name__)
            self.perform_action()
            return func(*args, **kwargs)
        return rate_limited_func

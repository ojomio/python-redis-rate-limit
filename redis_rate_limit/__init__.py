#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import datetime
import time
from distutils.version import StrictVersion
from hashlib import sha1

from redis import Redis, ConnectionPool
from redis.exceptions import NoScriptError

__version__ = "0.0.1"

# Adapted from http://redis.io/commands/incr#pattern-rate-limiter-2
INCREMENT_SCRIPT = b"""
    local current
    current = tonumber(redis.call("incr", KEYS[1]))
    if current == 1 then
        redis.call("expire", KEYS[1], ARGV[1])
    end
    return current
"""
INCREMENT_SCRIPT_HASH = sha1(INCREMENT_SCRIPT).hexdigest()

REDIS_POOL = ConnectionPool(host='127.0.0.1', port=6379, db=0)


class RedisVersionNotSupported(Exception):
    """
    Rate Limit depends on Redis’ commands EVALSHA and EVAL which are
    only available since the version 2.6.0 of the database.
    """
    pass


class TooManyRequests(Exception):
    """
    Occurs when the maximum number of requests is reached for a given resource
    of an specific user.
    """
    pass


class GaveUp(Exception):
    pass


class QuotaTimeout(Exception):
    pass


class RateLimit(object):
    """
    This class offers an abstraction of a Rate Limit algorithm implemented on
    top of Redis >= 2.6.0.
    """
    def __init__(self, resource, client, max_requests, expire=None, pessimistic_acquire=False, blocking=True, r_connection=None):
        """
        Class initialization method checks if the Rate Limit algorithm is
        actually supported by the installed Redis version and sets some
        useful properties.

        If Rate Limit is not supported, it raises an Exception.

        :param resource: resource identifier string (i.e. ‘user_pictures’)
        :param client: client identifier string (i.e. ‘192.168.0.10’)
        :param max_requests: integer (i.e. ‘10’)
        :param expire: seconds to wait before resetting counters (i.e. ‘60’)
        """
        if r_connection:
            self._redis = r_connection
        else:
            self._redis = Redis(connection_pool=REDIS_POOL)

        if not self._is_rate_limit_supported():
            raise RedisVersionNotSupported()

        self._rate_limit_key = "rate_limit:{0}_{1}".format(resource, client)
        self._num_waiting_key = "waiting_rate_limit:{0}_{1}".format(resource, client)

        self._max_requests = max_requests
        self._expire = expire or 1  # limit requests per this period of time
        self._acquire_overall_timeout = self._expire * 5  # if cannot acquire this long, fail
        self._acquire_check_timeout = self._expire / 10.  # if quota is empty, retry after this period
        self.acquired_times = 0  # number of times rate limiter was used
        self.acquire_attempt = 0  # current attempt to acquire quota

        self.blocking = blocking
        self.pessimistic_acquire = pessimistic_acquire

    def __enter__(self):
        if self.acquire_attempt > 0:
            raise GaveUp('Do not nest the usage of %r instance!' % self)

        if not self._max_requests:  # effectively do not control rate limit
            return

        if (self.pessimistic_acquire and
                self.acquired_times == 0 and  # new task
                self.has_been_reached()):  # quota is empty
            # don't try to acquire
            raise GaveUp(
                'Won\'t acquire quota as there are %d instances waiting for it already' %
                self.number_of_waiting_for_quota()
            )

        acquire_attempt_start = datetime.datetime.now()
        self.acquire_attempt = 1

        try:
            while True:
                if (self.blocking and
                        (datetime.datetime.now() - acquire_attempt_start).seconds > self._acquire_overall_timeout):
                    raise QuotaTimeout('Unable to acquire quota in %.2f secs' % self._acquire_overall_timeout)

                try:
                    self.increment_usage()
                except TooManyRequests:
                    if not self.blocking:
                        raise
                    else:
                        if self.acquire_attempt == 1:
                            self._redis.incr(self._num_waiting_key)  # +1 process waiting
                        self.acquire_attempt += 1
                        time.sleep(self._acquire_check_timeout)
                else:
                    self.acquired_times += 1
                    break
        except Exception:
            if self.acquire_attempt > 1:
                self._redis.decr(self._num_waiting_key)
            self.acquire_attempt = 0
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.acquire_attempt = 0

    def get_usage(self):
        """
        Returns actual resource usage by client. Note that it could be greater
        than the maximum number of requests set.

        :return: integer: current usage
        """
        return int(self._redis.get(self._rate_limit_key) or 0)

    def has_been_reached(self):
        """
        Checks if Rate Limit has been reached.

        :return: bool: True if limit has been reached or False otherwise
        """
        return self.get_usage() >= self._max_requests

    def number_of_waiting_for_quota(self):
        """
        Checks how much RateLimiter instances are waiting for quota
        :return: int: quantity
        """
        return int(self._redis.get(self._num_waiting_key) or 0)

    def increment_usage(self):
        """
        Calls a LUA script that should increment the resource usage by client.

        If the resource limit overflows the maximum number of requests, this
        method raises an Exception.

        :return: integer: current usage
        """
        # perform check first, so not even try to increment usage if not quota is left
        if self.has_been_reached():
            raise TooManyRequests()

        try:
            current_usage = self._redis.evalsha(
                INCREMENT_SCRIPT_HASH, 1, self._rate_limit_key, self._expire)
        except NoScriptError:
            current_usage = self._redis.eval(
                INCREMENT_SCRIPT, 1, self._rate_limit_key, self._expire)
        # Due to race condition,
        # several `increment_usage()` instances might have passed the initial check. Example:
        #
        # quota = 10
        # C1. check quota -> 9
        # C2. check quota -> 9
        # C1. incr -> 10
        # C2. incr -> 11 (over quota!)
        #
        # So we check the actual usage after increment, too

        if int(current_usage) > self._max_requests:
            raise TooManyRequests()

        return current_usage

    def _is_rate_limit_supported(self):
        """
        Checks if Rate Limit is supported which can basically be found by
        looking at Redis database version that should be 2.6.0 or greater.

        :return: bool
        """
        redis_version = self._redis.info()['redis_version']
        is_supported = StrictVersion(redis_version) >= StrictVersion('2.6.0')
        return bool(is_supported)

    def _reset(self):
        """
        Deletes all keys that start with ‘rate_limit:’.
        """
        for rate_limit_key in self._redis.keys('rate_limit:*'):
            self._redis.delete(rate_limit_key)


"""
Tests for tarantool queue.
"""
import threading

import tarantool
from tarantool_queue import Queue

from .testutils import QueueBaseTestCase


class SuperTarantoolConnection(tarantool.Connection):
    """
    Overriden tarantool connection.
    """
    pass


class FakeGoodConnection(object):
    """
    Good fake tarantool connection object.
    """
    def __init__(self):
        pass

    def call(self):
        pass


class FakeGoodLock(object):
    """
    Good fake lock object.
    """
    def __enter__(self):
        pass

    def __exit__(self):
        pass


class BadFake(object):
    """
    Bad fake for tarantool connection and lock objects.
    """
    pass


class QueueConnectionTestCase(QueueBaseTestCase):
    def test_connection(self):
        # get queue tarantool connection
        connection = self.queue.tnt

        # check if queue tarantool connection is the same
        self.assertEqual(connection, self.queue.tnt)

        # check if queue tarantool connection by default
        # is `tarantool.connection.Connection` instanse
        self.assertIsInstance(connection, tarantool.connection.Connection)
        self.assertIsInstance(self.queue.tnt, tarantool.connection.Connection)

    def test_connection_reset(self):
        # queue `_tnt` attribute must be None by default (right after connect)
        self.assertIsNone(self.queue._tnt)
        # touch queue `tnt` property (this will setup queue `_tnt` attribute)
        self.assertIsNotNone(self.queue.tnt)
        # queue `_tnt` attribute is not None now
        self.assertIsNotNone(self.queue._tnt)

        # set queue custom tarantool connection
        self.queue.tarantool_connection = SuperTarantoolConnection
        # queue connection must be `SuperTarantoolConnection` after setup
        self.assertEqual(self.queue.tarantool_connection,
                         SuperTarantoolConnection)

        # queue `_tnt` attribute must be None after setup tarantool connection
        self.assertIsNone(self.queue._tnt)
        # touch queue `tnt` property (this will setup queue `_tnt` attribute)
        self.assertIsNotNone(self.queue.tnt)
        # queue `_tnt` attribute is not None now
        self.assertIsNotNone(self.queue._tnt)

        # reset queue tarantool connection
        self.queue.tarantool_connection = None
        # queue connection must be `tarantool.Connection` by default
        self.assertEqual(self.queue.tarantool_connection,
                         tarantool.Connection)

        # queue `_tnt` attribute must be None after reset tarantool connection
        self.assertIsNone(self.queue._tnt)
        # touch queue `tnt` property (this will setup queue `_tnt` attribute)
        self.assertIsNotNone(self.queue.tnt)
        # queue `_tnt` attribute is not None now
        self.assertIsNotNone(self.queue._tnt)

    def test_connection_good(self):
        # set custom fake tarantool connection
        self.queue.tarantool_connection = FakeGoodConnection
        # queue tarantoo connection must be `FakeGoodConnection` after setup
        self.assertEqual(self.queue.tarantool_connection,
                         FakeGoodConnection)

        # reset queue tarantool connection
        self.queue.tarantool_connection = None
        # queue tarantool connection must be `tarantool.Connection` by default
        self.assertEqual(self.queue.tarantool_connection,
                         tarantool.Connection)

    def test_connection_bad(self):
        # if we will try to set bad tarantool connection for queue it will fall
        with self.assertRaises(TypeError):
            self.queue.tarantool_connection = BadFake

        # if we will try to set bad tarantool connection for queue it will fall
        with self.assertRaises(TypeError):
            self.queue.tarantool_connection = lambda x: x

        # queue tarantool connection must be `tarantool.Connection` anyway
        self.assertEqual(self.queue.tarantool_connection,
                         tarantool.Connection)


class QueueLockTestCase(QueueBaseTestCase):
    def setUp(self):
        # create new tarantool connection before each test
        self.queue = Queue('127.0.0.1', 33013, user='test', password='test')

    def test_lock_reset(self):
        # queue tarantool lock is `threading.Lock` by default
        self.assertIsInstance(self.queue.tarantool_lock,
                              type(threading.Lock()))

        # set queue tarantool lock to `threading.RLock`
        self.queue.tarantool_lock = threading.RLock()
        # queue tarantool lock is `threading.RLock` now
        self.assertIsInstance(self.queue.tarantool_lock,
                              type(threading.RLock()))

        # reset queue tarantool lock
        self.queue.tarantool_lock = None
        # queue tarantool lock is `threading.Lock` by default after reset
        self.assertIsInstance(self.queue.tarantool_lock,
                              type(threading.Lock()))

    def test_lock_good(self):
        # set custom fake queue tarantool lock
        self.queue.tarantool_lock = FakeGoodLock()
        # queue tarantool lock is `FakeGoodLock` now
        self.assertIsInstance(self.queue.tarantool_lock,
                              FakeGoodLock)

        # reset queue tarantool lock
        self.queue.tarantool_lock = None
        # queue tarantool lock is `threading.Lock` by default after reset
        self.assertIsInstance(self.queue.tarantool_lock,
                              type(threading.Lock()))

    def test_lock_bad(self):
        # if we will try to set bad tarantool lock for queue it will fall
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = BadFake

        # if we will try to set bad tarantool lock for queue it will fall
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = BadFake()

        # if we will try to set bad tarantool lock for queue it will fall
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = lambda x: x

        # queue tarantool lock is `threading.Lock` anyway
        self.assertIsInstance(self.queue.tarantool_lock,
                              type(threading.Lock()))

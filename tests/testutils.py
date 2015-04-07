"""
Test utils for tarantool queue.
"""
import unittest

from tarantool_queue import Queue


class QueueBaseTestCase(unittest.TestCase):
    """
    Base test case for queue tests.
    """
    def setUp(self):
        # create new tarantool connection before each test
        self.queue = Queue('127.0.0.1', 33016, user='test', password='test')


class TubeBaseTestCase(unittest.TestCase):
    """
    Base test case for queue tube tests.
    """
    @classmethod
    def setUpClass(cls):
        # connect to tarantool once
        cls.queue = Queue('127.0.0.1', 33016, user='test', password='test')

    def _cleanup_tube(self):
        """
        Delete all tasks in all queue tubes.
        """
        for tube in self.queue.tubes.values():
            task = tube.take(timeout=0)
            while task:
                task.delete()
                task = tube.take(timeout=0)

    def setUp(self):
        # delete all tasks in all tubes
        self._cleanup_tube()

    def tearDown(self):
        # report all tasks in all queue tubes as executed
        self._cleanup_tube()

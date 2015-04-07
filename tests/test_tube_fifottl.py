"""
Tests for tarantool queue 'fifottl' tube.
"""
import time

from tarantool_queue import Queue

from .test_tube_fifo import TubeFifoTestCase


class TubeFifoTTLTestCase(TubeFifoTestCase):
    """
    Tests for tarantool queue 'fifottl' tube.

    Inherit base tube tests from 'fifo' tube tests.
    We will implement tests only for few 'tubettl' features here.
    """
    tube_name = 'test_fifottl'

    @classmethod
    def setUpClass(cls):
        super(TubeFifoTestCase, cls).setUpClass()

        # connect to 'fifottl' test tube
        cls.tube = cls.queue.tube_fifottl(cls.tube_name)

    def test_task_put_ttl(self):
        # put new task in tube; set task 'time to live' to one second
        self.tube.put('foo', ttl=1)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # put new task in tube; set task 'time to live' to zero (dead task)
        self.tube.put('foo', ttl=0)
        # no tasks will be taken from tube (task is dead)
        self.assertIsNone(self.tube.take(timeout=0))

        # put new task in tube; set task 'time to live' to 0.1 second (float)
        self.tube.put('foo', ttl=0.1)
        # sleep more than 0.1 second
        time.sleep(0.2)
        # no tasks will be taken from tube (task is dead after 'ttl' seconds)
        self.assertIsNone(self.tube.take(timeout=0))

        # put new task in tube; set task 'time to live' to 1 second (integer)
        self.tube.put('foo', ttl=1)
        # sleep more than 1 second
        time.sleep(1.1)
        # no tasks will be taken from tube (task is dead after 'ttl' seconds)
        self.assertIsNone(self.tube.take(timeout=0))

        # put new task in tube; set task 'time to live' to 0.1 second
        self.tube.put('foo', ttl=0.1)
        # take and bury this task
        self.assertTrue(self.tube.take(timeout=0).bury())
        # sleep more than 0.1 second
        time.sleep(0.2)
        # no tasks will be kicked (task is dead after 'ttl' seconds')
        self.assertEqual(self.tube.kick(), 0)

    def test_task_put_ttl_delay(self):
        # put new task in tube
        # set task 'time to live' to 0.1 second and delay to 0.1 second
        task = self.tube.put('foo', ttl=0.1, delay=0.1)
        # this task status is 'delayed'
        self.assertEqual(task.status, '~')

        # sleep more than 0.1 second, but less, than 0.2 second
        time.sleep(0.15)

        # this task status is 'ready' now
        task.peek()
        self.assertEqual(task.status, 'r')

        # sleep more (0.1 second)
        time.sleep(0.15)

        # this task status is dead now (after 'ttl' + 'delay' seconds)
        with self.assertRaises(Queue.DatabaseError):
            task.peek()

        # no tasks will be taken (task is dead after 'ttl' + 'delay' seconds)
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_ttr(self):
        # put new task in tube; set task 'time to release' to one second
        self.tube.put('foo', ttr=1)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # put new task in tube; set task 'time to release' to 0.1 second
        self.tube.put('foo', ttr=0.1)
        # take this task from tube
        task = self.tube.take(timeout=0)
        # task was taken
        self.assertTrue(task)

        # sleep more than 0.1 second - task will released because 'ttr'
        time.sleep(0.2)

        # try to ack this task: error will be raised (task is released)
        with self.assertRaises(Queue.DatabaseError):
            task.ack()

        # take this task again
        task = self.tube.take(timeout=0)
        # task was taken
        self.assertTrue(task)
        # successfully ack this task
        task.ack()

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_priority_same(self):
        # put few tasks with equal priority in tube
        task11 = self.tube.put('foo', priority=1)
        task21 = self.tube.put('bar', priority=1)
        task31 = self.tube.put('baz', priority=1)

        # take all these tasks back
        task12 = self.tube.take(timeout=0)
        task22 = self.tube.take(timeout=0)
        task32 = self.tube.take(timeout=0)

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # check if tasks order is correct (first in, first out)
        self.assertEqual(task11.task_id, task12.task_id)
        self.assertEqual(task11.data, task12.data)
        self.assertEqual(task12.data, 'foo')

        self.assertEqual(task21.task_id, task22.task_id)
        self.assertEqual(task21.data, task22.data)
        self.assertEqual(task22.data, 'bar')

        self.assertEqual(task31.task_id, task32.task_id)
        self.assertEqual(task31.data, task32.data)
        self.assertEqual(task32.data, 'baz')

        # ack all tasks
        self.assertTrue(task12.ack())
        self.assertTrue(task22.ack())
        self.assertTrue(task32.ack())

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_priority_differ(self):
        # put few tasks with differ priority in tube
        task11 = self.tube.put('foo', priority=3)
        task21 = self.tube.put('bar', priority=2)
        task31 = self.tube.put('baz', priority=1)

        # take all these tasks back
        task32 = self.tube.take(timeout=0)
        task22 = self.tube.take(timeout=0)
        task12 = self.tube.take(timeout=0)

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # check if tasks order is correct
        # (lowest priority value for the highest task priority)
        self.assertEqual(task11.task_id, task12.task_id)
        self.assertEqual(task11.data, task12.data)
        self.assertEqual(task12.data, 'foo')

        self.assertEqual(task21.task_id, task22.task_id)
        self.assertEqual(task21.data, task22.data)
        self.assertEqual(task22.data, 'bar')

        self.assertEqual(task31.task_id, task32.task_id)
        self.assertEqual(task31.data, task32.data)
        self.assertEqual(task32.data, 'baz')

        # ack all tasks
        self.assertTrue(task12.ack())
        self.assertTrue(task22.ack())
        self.assertTrue(task32.ack())

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_put_delay(self):
        # put new task in tube; set task 'delay' to 0.1 second
        task = self.tube.put('foo', delay=0.1)
        # task status is 'delayed'
        self.assertEqual(task.status, '~')
        # no tasks yet in tube (delay is working)
        self.assertIsNone(self.tube.take(timeout=0))

        # sleep more than 0.2 second - task will be ready soon
        time.sleep(0.2)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_release_delay(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # release task and set 'delay' to 0.1 second
        self.assertTrue(task.release(delay=0.1))
        # task status is 'delayed'
        self.assertEqual(task.status, '~')
        # no tasks yet in tube (delay is working)
        self.assertIsNone(self.tube.take(timeout=0))

        # sleep more than 0.1 second - task will be ready soon
        time.sleep(0.2)
        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_status_delayed(self):
        # put new task in tube set task 'delay' to 0.1 second
        task = self.tube.put('foo', delay=0.1)
        # this task status is 'delayed'
        self.assertEqual(task.status, '~')
        self.assertEqual(task.status_name, 'delayed')

        # sleep more than 0.1 second - task will be ready soon
        time.sleep(0.2)

        # this task status is 'ready' now
        task.peek()
        self.assertEqual(task.status, 'r')
        self.assertEqual(task.status_name, 'ready')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # release task and set 'delay' to 0.1 second
        self.assertTrue(task.release(delay=0.1))
        # this task status is 'delayed'
        self.assertEqual(task.status, '~')
        self.assertEqual(task.status_name, 'delayed')

        # sleep more than 0.1 second - task will be ready soon
        time.sleep(0.2)

        # this task status is 'ready' now
        task.peek()
        self.assertEqual(task.status, 'r')
        self.assertEqual(task.status_name, 'ready')

        # take and ack this task
        self.assertTrue(self.tube.take(timeout=0).ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

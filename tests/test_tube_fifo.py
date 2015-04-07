"""
Tests for tarantool queue 'fifo' tube.
"""
import time

from tarantool_queue import Queue

from .testutils import TubeBaseTestCase


class TubeFifoTestCase(TubeBaseTestCase):
    """
    Tests for tarantool queue 'fifo' tube.

    This tests are base for all other tube types tests.
    """
    tube_name = 'test_fifo'

    @classmethod
    def setUpClass(cls):
        super(TubeFifoTestCase, cls).setUpClass()

        # connect to 'fifo' test tube
        cls.tube = cls.queue.tube_fifo(cls.tube_name)

    def test_tube(self):
        # get test 'fifo' tube and check it is equal with setuped tube
        tube1 = self.queue.tube_fifo(self.tube_name)
        self.assertEqual(self.tube, tube1)

        # get test 'fifo' tube again and check it is equal with setuped tube
        tube2 = self.queue.tube_fifo(self.tube_name)
        self.assertEqual(self.tube, tube2)

        # check if all tubes are equal
        self.assertEqual(tube1, tube2)

    def test_tasks_order(self):
        # put few tasks in tube
        task11 = self.tube.put('foo')
        task21 = self.tube.put('bar')
        task31 = self.tube.put('baz')

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

        # still no tasks in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_status(self):
        # put new task in tube
        task = self.tube.put('foo')
        # this task status is 'ready'
        self.assertEqual(task.status, 'r')
        self.assertEqual(task.status_name, 'ready')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # task was taken
        self.assertTrue(task)
        # task status is 'taken'
        self.assertEqual(task.status, 't')
        self.assertEqual(task.status_name, 'taken')

        # bury task
        self.assertTrue(task.bury())
        # task status is 'buried' now
        self.assertEqual(task.status, '!')
        self.assertEqual(task.status_name, 'buried')

        # kick buried task
        self.assertEqual(self.tube.kick(), 1)
        # take this task back from tube
        task = self.tube.take(timeout=0)
        # task was taken
        self.assertTrue(task)
        # task status is 'taken' now
        self.assertEqual(task.status, 't')
        self.assertEqual(task.status_name, 'taken')

        # ack this task
        self.assertTrue(task.ack())
        # task status is 'done' now
        self.assertEqual(task.status, '-')
        self.assertEqual(task.status_name, 'done')

    def test_task_take(self):
        # no tasks in tube within 'timeout' seconds (integer)
        time_start = time.time()
        self.assertIsNone(self.tube.take(timeout=1))
        self.assertTrue(1 <= time.time() - time_start < 2)

        # no tasks in tube within 'timeout' seconds (float)
        time_start = time.time()
        self.assertIsNone(self.tube.take(timeout=.3))
        self.assertTrue(.3 <= time.time() - time_start < .5)

        # put new task in tube
        self.tube.put('foo')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # this task status is 'taken'
        self.assertEqual(task.status, 't')

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # ack this task
        self.assertTrue(task.ack())

    def test_task_ack(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # ack this task
        self.assertTrue(task.ack())
        # this task status is 'done'
        self.assertEqual(task.status, '-')

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_release(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # release task
        self.assertTrue(task.release())
        # task status is 'ready'
        self.assertEqual(task.status, 'r')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # this task status is 'taken'
        self.assertEqual(task.status, 't')

        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_peek(self):
        # put new task in tube
        self.tube.put('foo')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # task status is now 'taken'
        self.assertEqual(task.status, 't')

        # release this task (this way, yes, we need to keep task status)
        self.queue.release(self.tube, task.task_id)

        # task status is still 'taken'
        self.assertEqual(task.status, 't')
        # peek task
        self.assertTrue(task.peek())
        # task status is now 'ready'
        self.assertEqual(task.status, 'r')

        # take this task from tube
        task = self.tube.take(timeout=0)
        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_bury(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # bury task
        self.assertTrue(task.bury())
        # task status is 'buried'
        self.assertEqual(task.status, '!')

        # kick buried task
        self.assertEqual(self.tube.kick(), 1)
        # take this task back from tube
        task = self.tube.take(timeout=0)
        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_kick(self):
        # put few tasks in tube
        self.tube.put('foo')
        self.tube.put('bar')
        self.tube.put('baz')

        # take first task from tube and bury it
        self.tube.take(timeout=0).bury()
        # take second task from tube and bury it
        self.tube.take(timeout=0).bury()
        # take third task from tube and bury it
        self.tube.take(timeout=0).bury()
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # kick two buried task
        self.assertEqual(self.tube.kick(count=2), 2)

        # take first task from tube and ack it
        self.tube.take(timeout=0).ack()
        # take second task from tube and ack it
        self.tube.take(timeout=0).ack()
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

        # kick two buried task (only one task left will buried)
        self.assertEqual(self.tube.kick(count=2), 1)

        # take third task from tube and ack it
        self.tube.take(timeout=0).ack()
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_delete(self):
        # put new task in tube
        self.tube.put('foo')
        # take this task from tube
        task = self.tube.take(timeout=0)

        # delete task
        self.assertTrue(task.delete())

        # task status is 'done'
        self.assertEqual(task.status, '-')

        # tack cannot be acked
        with self.assertRaises(Queue.DatabaseError):
            task.ack()

        # task cannot be buried
        with self.assertRaises(Queue.DatabaseError):
            task.bury()

        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

    def test_task_destructor(self):
        # put new task in tube
        task = self.tube.put('foo')

        # task is not taken - must not send exception
        task.__del__()

        # take this task from tube
        task = self.tube.take(timeout=0)

        # task in taken - must be released and not send exception
        task.__del__()

        # take this task back from tube
        task = self.tube.take(timeout=0)
        # ack this task
        self.assertTrue(task.ack())
        # no tasks left in tube
        self.assertIsNone(self.tube.take(timeout=0))

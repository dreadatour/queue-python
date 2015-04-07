# -*- coding: utf-8 -*-
"""
Python bindings for Tarantool queue script.

See also: http://github.com/tarantool/queue
"""
import threading

import tarantool


TASK_STATUS = {
    'r': 'ready',
    't': 'taken',
    '-': 'done',
    '!': 'buried',
    '~': 'delayed',
}


class Task(object):
    """
    Tarantool queue task wrapper.
    """
    def __init__(self, tube, task_id, status, data):
        self.tube = tube
        self.queue = tube.queue
        self.task_id = task_id
        self.status = status
        self.data = data

    def __str__(self):
        return "Task (id: {0}, status: {1})".format(self.task_id,
                                                    self.status_name)

    def __del__(self):
        if self.status == 't':
            try:
                self.release()
            except self.queue.DatabaseError:
                pass

    @property
    def status_name(self):
        """
        Returns status full name.
        """
        return TASK_STATUS.get(self.status, 'UNKNOWN')

    @classmethod
    def from_tuple(cls, tube, the_tuple):
        """
        Create queue task from tuple, returns `Task` instance.
        """
        if the_tuple is None:
            return

        if not the_tuple.rowcount:
            raise Queue.ZeroTupleException("error creating task")

        row = the_tuple[0]
        return cls(tube, task_id=row[0], status=row[1], data=row[2])

    def update_from_tuple(self, the_tuple):
        """
        Update task from tuple.
        """
        if not the_tuple.rowcount:
            raise Queue.ZeroTupleException("error updating task")

        row = the_tuple[0]
        self.status = row[1]
        self.data = row[2]

    def ack(self):
        """
        Report task successful execution.

        Returns `True` is task is acked (task status is 'done' now).
        """
        the_tuple = self.queue.ack(self.tube, self.task_id)
        self.update_from_tuple(the_tuple)

        return self.status == '-'

    def release(self, delay=None):
        """
        Put the task back into the queue.

        May contain a possible new `delay` before the task is executed again.

        Returns `True` is task is released
            (task status is 'ready' or 'delayed' if `delay` is set now).
        """
        the_tuple = self.queue.release(self.tube, self.task_id, delay=delay)
        self.update_from_tuple(the_tuple)

        if delay is None:
            return self.status == 'r'
        else:
            return self.status == '~'

    def peek(self):
        """
        Look at a task without changing its state.

        Always returns `True`.
        """
        the_tuple = self.queue.peek(self.tube, self.task_id)
        self.update_from_tuple(the_tuple)

        return True

    def bury(self):
        """
        Disable task until its restored again.
        """
        the_tuple = self.queue.bury(self.tube, self.task_id)
        self.update_from_tuple(the_tuple)

        return self.status == '!'

    def delete(self):
        """
        Delete task (in any state) permanently.
        """
        the_tuple = self.queue.delete(self.tube, self.task_id)
        self.update_from_tuple(the_tuple)

        return self.status == '-'


class Tube(object):
    """
    Tarantol queue tube wrapper.
    """
    def __init__(self, queue, name, **kwargs):
        self.queue = queue
        self.name = name
        self.options = kwargs.copy()

    def put(self, *args, **kwargs):
        """
        Enqueue a task.
        """
        raise NotImplementedError("can't use `Tube` abstract class")

    def take(self, timeout=None):
        """
        Get a task from queue for execution.

        Waits `timeout` seconds until a READY task appears in the queue.
        Returns either a `Task` object or `None`.
        """
        the_tuple = self.queue.take(self, timeout=timeout)

        if the_tuple.rowcount:
            return Task.from_tuple(self, the_tuple)

    def kick(self, count=None):
        """
        Reset back to READY status a bunch of buried task.
        """
        return self.queue.kick(self, count=count)

    def drop(self):
        """
        Drop entire query (if there are no in-progress tasks or workers).
        """
        return self.queue.drop(self)


class TubeFifo(Tube):
    """
    Tarantol queue 'fifo' tube wrapper.
    """
    def put(self, data):
        """
        Enqueue a task.

        Returns a `Task` object.
        """
        cmd = 'queue.tube.{0}:put'.format(self.name)
        args = (data,)

        the_tuple = self.queue.tnt.call(cmd, args)

        return Task.from_tuple(self, the_tuple)


class TubeFifoTTL(Tube):
    """
    Tarantol queue 'fifottl' tube wrapper.
    """
    def put(self, data, ttl=None, ttr=None, priority=None, delay=None):
        """
        Enqueue a task.

        @param ttl: time to live for a task put into the queue;
            if ttl is not given, it's set to infinity;
            "infinity" in tarantool is around 500 years (must be enough, yeah?)
        @param ttr: time allotted to the worker to work on a task;
            if not set, is the same as ttl
        @param priority: task priority;
            0 is the highest priority and is the default

        Returns a `Task` object.
        """
        cmd = 'queue.tube.{0}:put'.format(self.name)
        args = (data,)

        params = dict()
        if ttl is not None:
            params['ttl'] = ttl
        if ttr is not None:
            params['ttr'] = ttr
        if priority is not None:
            params['pri'] = priority
        if delay is not None:
            params['delay'] = delay
        if params:
            args += (params,)

        the_tuple = self.queue.tnt.call(cmd, args)

        return Task.from_tuple(self, the_tuple)


class Queue(object):
    """
    Tarantool queue wrapper.

    Usage:

        >>> from tarantool_queue import Queue
        >>> queue = Queue('127.0.0.1', 33013, user='test', password='test')
        >>> tube = queue.tube_fifo('holy_grail')
        # Put tasks into the queue
        >>> tube.put([1, 2, 3])
        >>> tube.put([2, 3, 4])
        # Get tasks from queue
        >>> task1 = tube.take()
        >>> task2 = tube.take()
        >>> print(task1.data)
            [1, 2, 3]
        >>> print(task2.data)
            [2, 3, 4]
        # Release tasks (put them back to queue)
        >>> del task2
        >>> del task1
        # Take task again
        >>> print(tube.take().data)
            [1, 2, 3]
        # Take task and mark it as complete
        >>> tube.take().ack()
            True
    """
    DatabaseError = tarantool.DatabaseError
    NetworkError = tarantool.NetworkError

    class BadConfigException(Exception):
        """
        Bad config queue exception.
        """
        pass

    class ZeroTupleException(Exception):
        """
        Zero tuple queue exception.
        """
        pass

    def __init__(self, host='localhost', port=33013, user=None, password=None):
        if not host or not port:
            raise Queue.BadConfigException(
                "host and port params must be not empty"
            )

        if not isinstance(port, int):
            raise Queue.BadConfigException("port must be int")

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tubes = {}
        self._lockinst = threading.Lock()
        self._conclass = tarantool.Connection
        self._tnt = None

    @property
    def tarantool_connection(self):
        """
        Tarantool connection getter.

        Uses `tarantool.Connection` by default.
        """
        if self._conclass is None:
            self._conclass = tarantool.Connection
        return self._conclass

    @tarantool_connection.setter
    def tarantool_connection(self, con_cls):
        """
        Tarantool connection setter.

        Must be class with methods call and __init__.
        """
        if con_cls is None:
            self._conclass = tarantool.Connection
        elif 'call' in dir(con_cls) and '__init__' in dir(con_cls):
            self._conclass = con_cls
        else:
            raise TypeError("Connection class must have connect"
                            " and call methods or be None")
        self._tnt = None

    @property
    def tarantool_lock(self):
        """
        Tarantool lock getter.

        Use `threading.Lock` by default.
        """
        if self._lockinst is None:
            self._lockinst = threading.Lock()

        return self._lockinst

    @tarantool_lock.setter
    def tarantool_lock(self, lock):
        """
        Tarantool lock setter.

        Must be locking instance with methods __enter__ and __exit__.
        """
        if lock is None:
            self._lockinst = threading.Lock()
        elif '__enter__' in dir(lock) and '__exit__' in dir(lock):
            self._lockinst = lock
        else:
            raise TypeError("Lock class must have `__enter__`"
                            " and `__exit__` methods or be None")

    @property
    def tnt(self):
        """
        Get or create tarantool connection.
        """
        if self._tnt is None:
            with self.tarantool_lock:
                if self._tnt is None:
                    self._tnt = self.tarantool_connection(
                        self.host,
                        self.port,
                        user=self.user,
                        password=self.password
                    )
        return self._tnt

    def take(self, tube, timeout=None):
        """
        Get a task from queue for execution.

        Waits `timeout` seconds until a READY task appears in the queue.
        If `timeout` is `None` - waits forever.

        Returns tarantool tuple object.
        """
        cmd = 'queue.tube.{0}:take'.format(tube.name)
        args = ()

        if timeout is not None:
            args += (timeout,)

        return self.tnt.call(cmd, args)

    def ack(self, tube, task_id):
        """
        Report task successful execution.

        Ack is accepted only from the consumer, which took the task
        for execution. If a consumer disconnects, all tasks taken
        by this consumer are put back to READY state (released).

        Returns tarantool tuple object.
        """
        cmd = 'queue.tube.{0}:ack'.format(tube.name)
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def release(self, tube, task_id, delay=None):
        """
        Put the task back into the queue.

        Used in case of a consumer for any reason can not execute a task.
        May contain a possible new `delay` before the task is executed again.

        Returns tarantool tuple object.
        """
        cmd = 'queue.tube.{0}:release'.format(tube.name)
        args = (task_id,)

        if delay is not None:
            args += ({'delay': delay},)

        return self.tnt.call(cmd, args)

    def peek(self, tube, task_id):
        """
        Look at a task without changing its state.

        Returns tarantool tuple object.
        """
        cmd = 'queue.tube.{0}:peek'.format(tube.name)
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def bury(self, tube, task_id):
        """
        Disable task until its restored again.

        If a worker suddenly realized that a task is somehow poisoned,
        can not be executed in the current circumstances, it can bury it.

        Returns tarantool tuple object.
        """
        cmd = 'queue.tube.{0}:bury'.format(tube.name)
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def kick(self, tube, count=1):
        """
        Reset back to READY status a bunch of buried task.

        Returns count of actually kicked tasts.
        """
        cmd = 'queue.tube.{0}:kick'.format(tube.name)
        args = (count,)

        the_tuple = self.tnt.call(cmd, args)

        if the_tuple.rowcount:
            return the_tuple[0][0]

        return 0

    def delete(self, tube, task_id):
        """
        Delete task (in any state) permanently.

        Returns tarantool tuple object.
        """
        cmd = 'queue.tube.{0}:delete'.format(tube.name)
        args = (task_id,)

        return self.tnt.call(cmd, args)

    def drop(self, tube):
        """
        Drop entire query (if there are no in-progress tasks or workers).

        Returns `True` on successful drop.
        """
        cmd = 'queue.tube.{0}:drop'.format(tube.name)
        args = ()

        the_tuple = self.tnt.call(cmd, args)

        return the_tuple.return_code == 0

    def tube_fifo(self, name):
        """
        Create 'fifo' tube object, if not created before.

        Returns `Tube` object.
        """
        tube = self.tubes.get(name)

        if tube is None:
            tube = TubeFifo(self, name)
            self.tubes[name] = tube

        return tube

    def tube_fifottl(self, name):
        """
        Create 'fifottl' tube object, if not created before.

        Returns `Tube` object.
        """
        tube = self.tubes.get(name)

        if tube is None:
            tube = TubeFifoTTL(self, name)
            self.tubes[name] = tube

        return tube

from __future__ import annotations
from logging import Logger
from collections import deque
from queue import Queue, Empty
from threading import Thread
from .extra import Singleton



def run_daemon_thread (*args, **kwargs):
    thread = Thread( *args, **kwargs )
    thread.daemon = True
    thread.start()
    return thread


class ThreadWorker ():
    __slots__ = set([ "_func", "_finished", "_result", "args", "kwargs", ])

    def __init__ (self, func, *args, **kwargs) -> None:
        self._func = func
        self._finished = False
        self._result = None
        self.args = args
        self.kwargs = kwargs

    @property
    def finished (self) -> bool:
        return self._finished

    @property
    def result (self):
        return self._result

    def run (self):
        self._finished = False
        self._result = None
        try:
            self._result = self._func( *self.args, **self.kwargs )
        except BaseException as exc:
            self._result = exc
            raise exc
        finally:
            self._finished = True
        return self._result


class ThreadPoolManager (metaclass=Singleton):
    __slots__ = set([ "_logger", "_queue", "_threads" ])

    def __init__ (self, size: int, logger: Logger) -> None:
        if logger:
            self._logger = logger
        self._queue = Queue()
        self._threads = deque()
        self._build_threads( size )

    def add_task (self, func, *args, **kwargs) -> ThreadWorker:
        worker = ThreadWorker( func, *args, **kwargs )
        q: Queue = self._queue
        q.put_nowait( worker )
        return worker

    def _build_threads (self, size: int):
        if len( self._threads ) > 0:
            return
        self._threads.extend(
            run_daemon_thread( target=self._task )
            for _
            in range( size )
        )

    def _task (self):
        q: Queue = self._queue
        while True:
            try:
                worker: ThreadWorker = q.get()
            except Empty:
                pass
            else:
                try:
                    worker.run()
                except BaseException as exc:
                    if self._logger:
                        self._logger.exception( exc, exc_info=True )
                    raise exc
                finally:
                    q.task_done()
                    continue

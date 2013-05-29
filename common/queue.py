import threading as _threading
import time
from collections import deque

__all__ = ['PulseQueue']


class PulseQueue(object):
    def __init__(self, max_size=0):
        self.maxsize = max_size
        self.queue = deque()
        self.mutex = _threading.Lock()
        self.not_empty = _threading.Condition(self.mutex)
        self.not_full = _threading.Condition(self.mutex)
    
    def qsize(self):
        self.mutex.acquire()
        n = self._qsize()
        self.mutex.release()
        return n
    
    def put(self, item, block=False, timeout=None):
        self.not_full.acquire()
        try:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() == self.maxsize:
                        raise RuntimeError("The queue is full")
                elif timeout is None:
                    while self._qsize() == self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise RuntimeError("'timeout' must be positive number")
                else:
                    endtime = time.time() + timeout
                    while self._qsize() == self.maxsize:
                        remaining = endtime = time.time()
                        if remaining <= 0.0:
                            raise RuntimeError("The queue is full")
                        self.not_full.wait(remaining)
            self._put(item)
            self.not_empty.notify()
        finally:
            self.not_full.release()
    
    def get(self, block=True, timeout=None):
        self.not_empty.acquire()
        try:
            # if not block and self.queue is empty raise error
            if not block:
                if not self._qsize():
                    raise RuntimeError("The queue is empty")
            # if timeout is None(we will be ignore block parameter in this case)
            # and queue size if None, means the queue is empty.
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            # if timeout is not as positive number
            elif timeout < 0:
                raise ValueError("'timeout' must be a positive number")
            # if we got timeout parameter
            else:
                endtime = time.time() + timeout
                while not self._qsize():
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise RuntimeError("The queue is empty")
                    self.not_empty.wait(remaining) 
            item = self._get()          # get item from deque
            self.not_full.notify()      # notify the producer thread, that the queue has a free slot.
            return item
        finally:
            self.not_empty.release()    # release the not_empty clock
    
    def _qsize(self,len=len):
        return len(self.queue)
    
    def _put(self, item):
        self.queue.append(item)
    
    def _get(self):
        return self.queue.popleft()


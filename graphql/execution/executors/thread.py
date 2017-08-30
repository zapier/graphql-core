from multiprocessing.pool import ThreadPool
from threading import Thread

from promise import Promise

from .utils import process


class ThreadExecutor(object):

    pool = None

    def __init__(self, pool=False):
        self.threads = []
        if pool:
            self.execute = self.execute_in_pool
            self.pool = ThreadPool(processes=pool)
        else:
            self.execute = self.execute_in_thread

    def execute_in_thread(self, fn, *args, **kwargs):
        promise = Promise()
        thread = Thread(target=process, args=(promise, fn, args, kwargs))
        thread.start()
        return promise

    def execute_in_pool(self, fn, *args, **kwargs):
        promise = Promise()
        self.pool.map(lambda input: process(*input), [(promise, fn, args, kwargs)])
        return promise

from multiprocessing import Process, Queue

from promise import Promise

from .utils import process


def queue_process(q):
    promise, fn, args, kwargs = q.get()
    process(promise, fn, args, kwargs)


class ProcessExecutor(object):

    def __init__(self):
        self.processes = []
        self.q = Queue()

    def execute(self, fn, *args, **kwargs):
        promise = Promise()
        self.q.put([promise, fn, args, kwargs], False)
        _process = Process(target=queue_process, args=(self.q))
        _process.start()
        return promise

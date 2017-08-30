from __future__ import absolute_import

import gevent
from promise import Promise

from .utils import process


class GeventExecutor(object):

    # def __init__(self):
    #     self.jobs = []

    def execute(self, fn, *args, **kwargs):
        promise = Promise()
        gevent.spawn(process, promise, fn, args, kwargs)
        # self.jobs.append(job)
        return promise

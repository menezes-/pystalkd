# -*- coding: utf8 -*-
"""pystalkd - A beanstalkd Client Library for Python3 - Based on https://github.com/earl/beanstalkc"""

__license__ = '''
Copyright (C) 2008-2014 Andreas Bolka
Copyright (c) 2019 Gabriel Menezes

MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''
__version__ = '1.3.0'


class Job:
    def __init__(self, connection, job_id, body, size, reserved=True):
        """
        Class representing a Job from beanstalkd
        `body` can be a bytes instance if it was used with put_bytes
        based on https://github.com/earl/beanstalkc/blob/master/beanstalkc.py#L255
        :param connection: Beanstalkd connection
        :type connection: pystalkd.Beanstalkd.Connection
        :param job_id: Job id return by put
        :type job_id: int
        :param body: Body of job
        :type body: str | bytes
        :param reserved: job is reserved or not
        :type reserved: bool
        :param size: size in bytes of the job body
        :type size: int
        """
        self.size = size
        self.body = body
        self.reserved = reserved
        self.job_id = job_id
        self.connection = connection

    def _priority(self):
        stats = self.stats()
        if isinstance(stats, dict):
            return stats['pri']
        return 2 ** 31

    def delete(self):
        """Delete this job."""
        self.connection.delete(self.job_id)
        self.reserved = False

    def release(self, priority=None, delay=0):
        """Release a reserved job back into the ready queue.
        See <https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#release-command> for full info.
        :param priority: new priority to assign to the job.
        :param delay: number of seconds to wait before putting the job in the ready queue.
        The job will be in the "delayed" state during this time.
        :type priority: int
        :type delay: int | timedelta
        """
        if self.reserved:
            self.connection.release(self.job_id, priority or self._priority(), delay)
            self.reserved = False

    def bury(self, priority=None):
        """Bury a job, by job id.
        See <https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#bury-command> for full info.
        :param priority: new priority to assign to the job.
        :type priority: int
        """
        if self.reserved:
            self.connection.bury(self.job_id, priority or self._priority())
            self.reserved = False

    def kick(self):
        """If the given job exists and is in a buried or
        delayed state, it will be moved to the ready queue of the the same tube where it currently belongs
        See <https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#kick-job-command> for full info.
        :return: job_id
        :rtype: int
        """
        self.connection.kick_job(self.job_id)

    def touch(self):
        """Touch a this job requesting more time to work
        See <https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#touch-command> for full info.
        """
        if self.reserved:
            self.connection.touch(self.job_id)

    def stats(self):
        """Return a dict of stats about this job.
        See <https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#stats-job-command> for full info.
        """
        return self.connection.stats_job(self.job_id)

# -*- coding: utf8 -*-
"""pystalkd - A beanstalkd Client Library for Python3 - Based on https://github.com/earl/beanstalkc"""

__license__ = '''
Copyright (C) 2008-2014 Andreas Bolka
Copyright (C) 2014 Gabriel Menezes
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
__version__ = '1.1'


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
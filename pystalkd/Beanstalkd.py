# -*- coding: utf8 -*-

"""pystalkd - A beanstalkd Client Library for Python3 - Based on https://github.com/earl/beanstalkc"""
from contextlib import contextmanager

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

import socket
from datetime import timedelta
from .Job import Job

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 11300
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120


class BeanstalkdException(Exception): pass


class UnexpectedResponse(BeanstalkdException): pass


class CommandFailed(BeanstalkdException): pass


class DeadlineSoon(BeanstalkdException): pass


class SocketError(BeanstalkdException):
    """
    good idea copied from: https://github.com/earl/beanstalkc/blob/master/beanstalkc.py#L37
    """

    @staticmethod
    def wrap(wrapped_function, *args, **kwargs):
        try:
            return wrapped_function(*args, **kwargs)
        except socket.error as err:
            raise SocketError(err)


def total_seconds(td):
    """
    simulates the total_seconds function added in python 3.2+ and 2.7+
    See
    :param td: timedelta
    :type td: timedelta
    :return: int representing total seconds from timedelta
    :rtype: int
    """
    #microseconds is not used, since i have to convert it to int to use in beanstalkd
    return int(((td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6)


class Connection(object):
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, parse_yaml=True,
                 connect_timeout=socket.getdefaulttimeout()):
        self.port = port
        self.host = host
        if parse_yaml:
            try:
                import yaml
            except ImportError:
                parse_yaml = False

        self.parse_yaml = parse_yaml

        self.server_errors = ["OUT_OF_MEMORY", "INTERNAL_ERROR", "BAD_FORMAT", "UNKNOWN_COMMAND"]

        self._connect_timeout = connect_timeout
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()

    def connect(self):
        """Connect to beanstalkd server."""
        if not self._socket:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._connect_timeout)
        SocketError.wrap(self._socket.connect, (self.host, self.port))

    def close(self):
        """close connection and send exit to beanstalkd server"""
        command = b"quit\r\n"
        try:
            self._socket.sendall(command)
            self._socket.close()
        except socket.error:
            pass

    def reconnect(self):
        self.close()
        self._socket = None
        self.connect()

    def _recv(self):
        """
        Return response from beanstalkd
        **WARNING**: if your data has '\r\n' your data my come corrupted, since \r\n is the final delimiter for
         beanstalkd. Encode your data.
        :return: response
        :rtype: bytes

        """
        buffer_size = 8192
        recvbuff = bytearray(buffer_size)
        mem_view = memoryview(recvbuff)

        while True:

            n_bytes = SocketError.wrap(self._socket.recv_into, mem_view)

            mem_view = mem_view[0:n_bytes]
            if mem_view[-2:] == b'\r\n':
                break

        return mem_view.tobytes()

    def send(self, command, *args):
        """
        Low-level send command. It sends the `command` string with the arguments present in `args`
        :param command: beanstalkd command i.e "put"
        :param args: arguments to the command
        :return: string with beanstalkd return
        :rtype: (str, bytearray)
        """
        tokens = [command] + [str(x, ) if not isinstance(x, str) else x for x in args]
        command = bytes(" ".join(tokens), "utf8") + b'\r\n'
        SocketError.wrap(self._socket.sendall, command)

        response = self._recv()
        response = response.strip().split(maxsplit=1)
        if len(response) == 1:
            status, rest = response[0], response[0]
        else:
            status, rest = response

        status = status.decode("utf8")
        if status in self.server_errors:
            raise BeanstalkdException(status)
        return status, rest

    def send_command(self, command, *args, ok_status=None, error_status=None):
        """
        Send the `command` to beanstalkd server and validate the response based on `ok_statyus` and `error_status`
        :param command: command to be sent
        :type command: str
        :param args: arguments to the command
        :type args: list
        :param ok_status: status that indicate a successful request
        :type ok_status: list of str
        :param error_status: status that indicate an error
        :type error_status: list of str
        :rtype: (str, bytearray)
        """
        status, command_body = self.send(command, *args)

        if not ok_status:
            ok_status = []
        if not error_status:
            error_status = []

        if status in ok_status:
            return status, command_body
        elif status in error_status:
            raise CommandFailed(status)
        else:
            raise UnexpectedResponse(status)

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        """
        Put a job into the current tube. Returns job id.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#put-command for full info.
        :param body: body of job
        :type body: str
        :param priority: priority of the job. Defaults to 2**31
        :type priority: long
        :param delay: number of seconds to wait before putting the job in the ready queue
        :type delay: int | timedelta
        :param ttr:  number of seconds to allow a worker to run this job
        :type ttr: int | timedelta
        :return: job id
        :rtype: int

        """
        assert isinstance(body, str), 'Job body must be a str instance'
        if isinstance(ttr, timedelta):
            ttr = total_seconds(ttr)
        if isinstance(delay, timedelta):
            delay = total_seconds(delay)
        ok_status = ['INSERTED']
        error_status = ['JOB_TOO_BIG', 'BURIED', 'DRAINING', 'EXPECTED_CRLF']
        status, job = self.send_command("put", priority, delay, ttr, str(len(body.encode("utf8"))) + "\r\n" + body,
                                        ok_status=ok_status,
                                        error_status=error_status)

        return int(job)

    def parse_job(self, body):
        job_id, body_rest = body.split(maxsplit=1)
        job_body_size, job_body = body_rest.split(maxsplit=1)
        job_body = str(job_body, "utf8")
        job_body_size = job_body_size
        return Job(self, int(job_id), job_body, int(job_body_size))

    def reserve(self, timeout=None):
        """
        Reserve a job from one of the watched tubes, with optional timeout
        in seconds. Returns a Job object, or None if the request times out.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#reserve-command for full info.
        :type timeout: int | timedelta
        :return: will return a newly-reserved job
        :rtype: Job
        """
        if isinstance(timeout, timedelta):
            timeout = total_seconds(timeout)

        if timeout is None:
            command = "reserve"
            args = []
        else:
            command = "reserve-with-timeout"
            args = [timeout, ]
        ok_status = ["RESERVED", "DEADLINE_SOON", "TIMED_OUT"]
        status, body = self.send_command(command, *args, ok_status=ok_status)
        if status == "TIMED_OUT":
            return None
        elif status == "DEADLINE_SOON":
            raise DeadlineSoon(body)

        return self.parse_job(body)

    def kick(self, bound=1):
        """Kick at most bound jobs into the ready queue.
        If there are any buried jobs, it will only kick buried jobs.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#kick-command for full info.
        Otherwise it will kick delayed jobs
        :return: count of kicked jobs
        :rtype: int
        """
        _, body = self.send_command("kick", bound, ok_status=["KICKED", ])
        return int(body)

    def kick_job(self, job_id):
        """If the given `job_id` exists and is in a buried or
        delayed state, it will be moved to the ready queue of the the same tube where it currently belongs
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#kick-job-command for full info.
        :param job_id: id of job
        :type job_id: int
        :return: job_id
        :rtype: int
        """
        _, body = self.send_command("kick-job", job_id, ok_status=["KICKED"],
                                    error_status=["NOT_FOUND"])

    def delete(self, job_id):
        """
        Delete a job, by job id.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#delete-command for full info.
        :param job_id: id of job
        :type job_id: int
        """

        self.send_command("delete", job_id, ok_status=["DELETED", ], error_status=["NOT_FOUND", ])

    def peek(self, job_id):
        """Peek at job `job_id`
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#other-commands for full info.
        :param job_id: id of job
        :type job_id: int
        :return: Job if found else None
        :rtype: Job | None
        """

        status, body = self.send_command('peek', job_id, ok_status=["NOT_FOUND", "FOUND"])

        if status == "NOT_FOUND":
            return None

        return self.parse_job(body)

    def _peek_state(self, state):
        """
        Generate and execute peek-* commands
        :param state: state to peek
        :type state: str
        :return:
        """
        command = "peek" + "-" + state
        status, body = self.send_command(command, ok_status=["NOT_FOUND", "FOUND"])
        if status == "NOT_FOUND":
            return None

        return self.parse_job(body)

    def peek_ready(self):
        """Peek at next ready job. Returns a Job, or None.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#peek-command for full info.
        :return: Job if found else None
        :rtype: Job | None
        """
        return self._peek_state("ready")

    def peek_delayed(self):
        """Peek at next delayed job. Returns a Job, or None.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#peek-command for full info.
        :return: Job if found else None
        :rtype: Job | None
        """
        return self._peek_state('delayed')

    def peek_buried(self):
        """Peek at next buried job. Returns a Job, or None.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#peek-command for full info.
        :return: Job if found else None
        :rtype: Job | None
        """
        return self._peek_state('buried')

    def _parse_yaml(self, body):

        body_size, body = body.split(maxsplit=1)
        body = str(body, "utf8")
        if not self.parse_yaml:
            return body

        import yaml

        return yaml.load(body)

    def tubes(self):
        """Return a list of all existing tubes.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#list-tubes-command for full info.
        if `parse_yaml` is True and pyyaml extension is installed it will parse the yaml an return a list else it will
        return the yaml string
        :return: list of all tubes
        :rtype: list of str | str
        """
        _, body = self.send_command("list-tubes", ok_status=["OK"])
        return self._parse_yaml(body)

    def using(self):
        """Return the tube currently being used.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#list-tube-used-command for full info.
        :return: current tube being used
        :rtype: str
        """
        _, body = self.send_command("list-tube-used", ok_status=["USING"])
        return str(body, "utf8")

    def use(self, name):
        """Use a `name` tube.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#use-command for full info.
        :param name: name of the tube
        :type name: str
        :return: current tube
        :rtype: str
        """
        _, body = self.send_command("use", name, ok_status=["USING"])
        return str(body, "utf8")

    @contextmanager
    def temporary_use(self, name):
        """
        Use a `name` tube temporarily and then go back to the previous one
        :param name: name of the tube
        :type name: str
        """
        old = self.using()
        self.use(name)
        yield
        self.use(old)

    def _check_name_size(self, name):
        """
        for validation. To prevent big names on some commands
        :param name: name to be validated
        :type name: str
        """
        temp_b = bytes(name, "utf8")
        if len(temp_b) > 200:
            raise ValueError("name must be at most 200 bytes")

    def watch(self, name):
        """Watch a given tube.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#watch-command for full info.
        :return: number of tubes currently in the watch list.
        :rtype: int
        :param name: name of the tube
        :type name: str
        """
        self._check_name_size(name)

        _, body = self.send_command("watch", name, ok_status=["WATCHING"])
        return int(body)

    @contextmanager
    def temporary_watch(self, name):
        """
        Watch a given tube and then ignores it. To be used in with statements.
        :param name: name of tube
        :type name: str
        """
        self.watch(name)
        yield
        self.ignore(name)

    def watching(self):
        """Return a list of all tubes being watched.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#list-tubes-watched-command for full info.
        if `parse_yaml` is True and pyyaml extension is installed it will parse the yaml an return a list else it will
        return the yaml string
        :return: all tubes being watched
        :rtype: list of str | str
        """
        _, body = self.send_command("list-tubes-watched", ok_status=["OK"])
        return self._parse_yaml(body)

    def ignore(self, name):
        """Stop watching a given tube.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#ignore-command for full info.
        :param name: name of tube
        :type name: str
        :return: number of tubes currently in the watch list.
        :rtype: int
        """
        self._check_name_size(name)

        _, body = self.send_command("ignore", name, ok_status=["WATCHING"], error_status=["NOT_IGNORED"])

        return int(body)

    def stats(self):
        """Return a dict of beanstalkd statistics.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#stats-command for full info.
        if `parse_yaml` is True and PyYaml extension is installed it will parse the yaml an return a dict else it will
        return the yaml string
        :return:  beanstalkd statistics
        :rtype: dict | str
        """
        _, body = self.send_command("stats", ok_status=["OK"])
        return self._parse_yaml(body)

    def stats_tube(self, name):
        """Return a dict of stats about a given tube.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#stats-tube-command for full info.
         if `parse_yaml` is True and PyYaml extension is installed it will parse the yaml an return a dict else it will
        return the yaml string
        :param name: tube
        :type name: str
        :return: stats about tube `name`
        :rtype: dict or str

        """
        self._check_name_size(name)
        _, body = self.send_command("stats-tube", name, ok_status=["OK"], error_status=["NOT_FOUND"])
        return self._parse_yaml(body)

    def pause_tube(self, name, delay):
        """Pause a tube for a given delay time, in seconds.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#pause-tube-command for full info.
        :param name: tube name
        :param delay: seconds to delay tube
        :type name: str
        :type delay: int | timedelta
        """

        if isinstance(delay, timedelta):
            delay = total_seconds(delay)

        self.send_command("pause-tube", name, delay, ok_status=["PAUSED"], error_status=["NOT_FOUND"])

    def release(self, job_id, priority=DEFAULT_PRIORITY, delay=0):
        """Release a reserved job back into the ready queue.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#release-command for full info.
        :param job_id: job id
        :param priority: new priority to assign to the job.
        :param delay: number of seconds to wait before putting the job in the ready queue.
        The job will be in the "delayed" state during this time.
        :type job_id: int
        :type priority: int
        :type delay: int | timedelta

        """
        if isinstance(delay, timedelta):
            delay = total_seconds(delay)
        # BURIED is considered an error because, acording to the protocol, "BURIED\r\n if the server ran out of memory trying to grow the priority queue data structure."
        self.send_command("release", job_id, priority, delay, ok_status=["RELEASED"],
                          error_status=["BURIED", "NOT_FOUND"])

    def bury(self, job_id, priority=DEFAULT_PRIORITY):
        """Bury a job, by job id.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#bury-command for full info.
        :param job_id: job id
        :param priority: new priority to assign to the job.
        :type job_id: int
        :type priority: int

        """
        self.send_command("bury", job_id, priority, ok_status=["BURIED"], error_status=["NOT_FOUND"])

    def touch(self, job_id):
        """Touch a job, by `job_id`, requesting more time to work on a reserved
        job before it expires.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#touch-command for full info.
        :param job_id: job id
        :type job_id: int
        """
        self.send_command("touch", job_id, ok_status=["TOUCHED"], error_status=["NOT_FOUND"])

    def stats_job(self, job_id):
        """Return a dict of stats about a job, by job id.
        See https://github.com/kr/beanstalkd/blob/master/doc/protocol.md#stats-job-command for full info.
        if `parse_yaml` is True and PyYaml extension is installed it will parse the yaml an return a dict else it will
        return the yaml string
        :param job_id: job id
        :type job_id: int
        :return: stats about the job
        :rtype: dict | str

        """
        _, body = self.send_command("stats-job", job_id, ok_status=["OK"], error_status=["NOT_FOUND"])
        return self._parse_yaml(body)







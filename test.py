from datetime import timedelta
from pystalkd import Beanstalkd
from os import urandom
import json
import random
import string
import unittest

__author__ = 'Gabriel'


# Todo: don't skip tests if PyYaml is not installed


def clean_tube(tube_name, conn):
    assert (isinstance(conn, Beanstalkd.Connection))
    conn.watch(tube_name)

    if conn.parse_yaml:
        stats = conn.stats()
        buried = stats["current-jobs-buried"]
        if buried > 0:
            conn.kick(buried)
    while True:
        # changed to reserve_bytes, because reserve will try to convert to a string
        # causing a UnicodeDecodeError: 'utf-8' codec can't decode byte 0xb9 in position 0: invalid start byte
        # basically: you can reserve a string as bytes but the other way around might cause problems
        job = conn.reserve_bytes(0)
        if job:
            job.delete()
        else:
            break


class TestBeanstalkd(unittest.TestCase):
    def __init__(self, testname, host=Beanstalkd.DEFAULT_HOST, port=Beanstalkd.DEFAULT_PORT):
        super(TestBeanstalkd, self).__init__(testname)
        self.host = host
        self.port = port

    def setUp(self):
        self.conn = Beanstalkd.Connection(self.host, self.port)
        self.tube_name = "pystalkd.tests"
        self.data = {"a": "b"}

    def step1(self):
        """
        test use
        """
        self.conn.use(self.tube_name)
        self.assertEqual(self.conn.using(), self.tube_name, "couldn't change using tube")

    def step2(self):
        """
        test watch
        """
        self.conn.watch(self.tube_name)
        watching = self.conn.watching()
        if self.conn.parse_yaml:
            should_be = ["default", self.tube_name]
            self.assertListEqual(watching, should_be, "couldn't change watching list")
        else:
            self.assertIn(self.tube_name, watching)

    def step3(self):
        """
        test ignore
        """
        self.conn.ignore("default")
        watching = self.conn.watching()

        if self.conn.parse_yaml:
            should_be = [self.tube_name]
            self.assertListEqual(watching, should_be, "couldn't ignore")
        else:
            self.assertNotIn("default", watching, "couldn't ignore")

    def step4(self):
        """
        test put
        """
        job_id = self.conn.put(json.dumps(self.data))
        self.assertIsInstance(job_id, int, "where's my job id?!")

    def step5(self):
        """
        test get, release
        """
        job = self.conn.reserve(0)
        self.assertIsNotNone(job, "should be a job here")
        self.assertDictEqual(self.data, json.loads(job.body), "Corrupted job body")
        job.release()
        if self.conn.parse_yaml:
            self.assertEqual(job.stats()["state"], "ready")

    def step7(self):
        job = self.conn.reserve(0)
        self.assertIsNotNone(job, "should be a job here")
        job.delete()
        self.assertIsNone(self.conn.reserve(0))
        with self.assertRaises(Beanstalkd.CommandFailed):
            job.stats()
        del job

    def step8(self):
        self.conn.put("hey!")
        job = self.conn.reserve(0)
        job.bury()
        should_be_same = self.conn.peek_buried()
        self.assertIsNotNone(should_be_same, "should be a job")
        self.assertEqual(should_be_same.body, "hey!", "different body")

    def step9(self):
        self.conn.kick(1)
        job = self.conn.reserve()
        self.assertIsNotNone(job, "should be a job")
        self.assertEqual(job.body, "hey!", "diferent body")
        job.delete()

    def test_wrong_connection(self):
        with self.assertRaises(Beanstalkd.SocketError):
            Beanstalkd.Connection("255.255.255.255")

    def test_temporary_use(self):
        conn = Beanstalkd.Connection(self.host, self.port)
        with conn.temporary_use(self.tube_name):
            self.assertEqual(conn.using(), self.tube_name, "should be using {}".format(self.tube_name))
        self.assertEqual(conn.using(), "default", "should be using 'default'")
        conn.close()

    def test_temporary_watch(self):
        conn = Beanstalkd.Connection(self.host, self.port)
        if conn.parse_yaml:
            with conn.temporary_watch(self.tube_name):
                self.assertListEqual(conn.watching(), ["default", self.tube_name],
                                     "not watching {}".format(self.tube_name))
            self.assertListEqual(conn.watching(), ["default"], "should only be watching 'default'")
        else:
            self.skipTest("needs PyYaml")
        conn.close()

    def test_no_yaml(self):
        conn = Beanstalkd.Connection(self.host, self.port, parse_yaml=False)
        conn.use(self.tube_name)

        self.assertIsInstance(conn.stats(), str)
        self.assertIsInstance(conn.stats_tube(self.tube_name), str)
        self.assertIsInstance(conn.tubes(), str)
        self.assertIsInstance(conn.watching(), str)
        conn.close()

    def test_timedelta(self):
        conn = Beanstalkd.Connection(self.host, self.port)
        conn.use(self.tube_name)
        if not conn.parse_yaml:
            self.skipTest("needs PyYaml")
            return
        t = timedelta(days=1)
        # 'ttr': 86400
        conn.put("hey!", ttr=t)
        job = conn.reserve()
        ttr = job.stats()["ttr"]
        self.assertEqual(ttr, 86400, "timedelta was not converted to seconds")

    def test_chinese_word(self):
        """
        test chinese
        """
        job_id = self.conn.put("台灣繁體字 Traditional Chinese characters")
        self.assertIsInstance(job_id, int, "where's my job id?!")

        job = self.conn.reserve(0)

        self.assertEqual(job.body, "台灣繁體字 Traditional Chinese characters")
        job.delete()

    def test_bytes(self):
        """
        test put bytes and reserve bytes
        """

        # test with random bytes
        test_bytes = urandom(50)
        job_id = self.conn.put_bytes(test_bytes)
        self.assertIsInstance(job_id, int, "where's my job id?!")

        job = self.conn.reserve_bytes(0)
        self.assertEqual(job.body, test_bytes)
        job.delete()

    def test_big(self):
        """
        Test a job with the max allowed size for job
        """
        if self.conn.parse_yaml:
            max_size = self.conn.stats()['max-job-size']
        else:
            # use the default max size
            max_size = 65535  # bytes

        test_str = ''.join(random.choice(string.ascii_uppercase) for _ in range(max_size))
        job_id = self.conn.put(test_str)
        self.assertIsInstance(job_id, int, "where's my job id?!")
        job = self.conn.reserve(0)
        body = job.body
        job.delete()
        self.assertEqual(test_str, body)

    def test_big_bytes(self):
        if self.conn.parse_yaml:
            max_size = self.conn.stats()['max-job-size']
        else:
            # use the default max size
            max_size = 65535  # bytes

        test_bytes = urandom(max_size)
        job_id = self.conn.put_bytes(test_bytes)
        self.assertIsInstance(job_id, int, "where's my job id?!")
        job = self.conn.reserve_bytes(0)
        body = job.body
        job.delete()
        self.assertEqual(test_bytes, body)

    def test_infinite_loop(self):
        # get current job_id
        hello_world = b'hello_world'
        job_id = self.conn.put_bytes(hello_world)
        job = self.conn.reserve_bytes(0)
        job.delete()

        # calculate size for infinite loop, so that
        # \r and \n will be in different buffers
        size = 4096 - len('RESERVED {} 4095\r\n'.format(job_id)) - len('\r')
        test_bytes = urandom(size)
        job_id = self.conn.put_bytes(test_bytes)
        self.assertIsInstance(job_id, int, "where's my job id?!")
        job = self.conn.reserve_bytes(0)
        body = job.body
        job.delete()
        self.assertEqual(test_bytes, body)

    # http://stackoverflow.com/a/5387956/482238

    def steps(self):
        for name in sorted(dir(self)):
            if name.startswith("step"):
                yield name, getattr(self, name)

    def test_steps(self):
        for name, step in self.steps():
            try:
                step()
            except Exception as e:
                self.fail("{} failed ({}: {})".format(step, type(e), e))

    def tearDown(self):
        # clean the queue
        clean_tube(self.tube_name, self.conn)
        self.conn.close()


if __name__ == '__main__':
    import sys

    try:
        host_arg = sys.argv[1]
    except IndexError:
        host_arg = Beanstalkd.DEFAULT_HOST

    try:
        port_arg = sys.argv[2]
    except IndexError:
        port_arg = Beanstalkd.DEFAULT_PORT

    suite = unittest.TestSuite()
    suite.addTest(TestBeanstalkd("test_steps", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_wrong_connection", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_no_yaml", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_temporary_use", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_temporary_watch", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_chinese_word", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_bytes", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_big", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_big_bytes", host_arg, port_arg))
    suite.addTest(TestBeanstalkd("test_infinite_loop", host_arg, port_arg))
    unittest.TextTestRunner().run(suite)

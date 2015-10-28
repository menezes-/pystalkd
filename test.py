from datetime import timedelta

__author__ = 'Gabriel'

from pystalkd import Beanstalkd
import json
import unittest

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
        job = conn.reserve(0)
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
            self.assertListEqual(conn.watching(), ["default"], "should only be watchin 'default'")
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
        #'ttr': 86400
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

        self.assertEquals(job.body, "台灣繁體字 Traditional Chinese characters")
        job.delete()

    def test_bytes(self):
        """
        test put bytes and reserve bytes
        """
        test_bytes = "台灣繁體字 Traditional Chinese characters".encode("utf8")
        job_id = self.conn.put_bytes(test_bytes)
        job = self.conn.reserve_bytes(0)
        self.assertEquals(job.body, test_bytes)
        job.delete()

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
        #clean the queue
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
    unittest.TextTestRunner().run(suite)

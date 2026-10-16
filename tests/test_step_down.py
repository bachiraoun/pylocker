"""Tests that a server which cannot keep its server file fresh stops, and the smaller fixes of the connect and publish code."""
import os
import time
import unittest
from unittest import mock

from pylocker import ServerLocker
from pylocker import Locker as lockerModule

from helpers import LockerTestCase, PASSWORD, run_child, wait_until


class TestServerSteppingDown(LockerTestCase):
    """Checks that two servers never run at once because one lost its server file."""

    def setUp(self):
        """Makes the server file refresh every 0.3 seconds, so a server leaves within about a second."""
        super().setUp()
        patcher = mock.patch.object(lockerModule, '_HEARTBEAT_INTERVAL', 0.3)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_server_stops_when_its_file_cannot_be_written(self):
        """Stops serving after the file stayed out of reach for two beats, so nobody else serves at the same time."""
        server = self.make_locker(self.server_file())
        self.assertTrue(server.isServer)
        os.chmod(server.serverFile, 0o660)
        self.assertTrue(wait_until(lambda: not server.isServer, seconds=5))

    def test_next_locker_serves_after_the_server_stepped_down(self):
        """Lets another locker become the only server once the file is usable again."""
        server = self.make_locker(self.server_file())
        os.chmod(server.serverFile, 0o660)
        self.assertTrue(wait_until(lambda: not server.isServer, seconds=5))
        os.chmod(server.serverFile, 0o600)
        other = self.make_locker(server.serverFile)
        self.assertTrue(other.isServer)

    def test_one_failed_beat_is_forgiven(self):
        """Keeps serving when only one refresh of the file failed."""
        server = self.make_locker(self.server_file())
        real = ServerLocker._write_fingerprint
        failures = [0]

        def fail_once(locker, port):
            if failures[0] == 0:
                failures[0] += 1
                return False
            return real(locker, port)
        with mock.patch.object(ServerLocker, '_write_fingerprint', fail_once):
            self.assertTrue(wait_until(lambda: failures[0] == 1, seconds=3))
            time.sleep(1.5)
        self.assertTrue(server.isServer)

    def test_server_stops_when_the_file_names_another_server(self):
        """Stops serving when another locker took over the server file."""
        server = self.make_locker(self.server_file())
        line = lockerModule._format_fingerprint('someone-else', time.time(), '127.0.0.1', 5999, os.getpid())
        lockerModule._write_server_file_text(server.serverFile, line + '\n')
        self.assertTrue(wait_until(lambda: not server.isServer, seconds=5))

    def test_write_reports_failure(self):
        """Reports False when the server file is refused, and True when it was written."""
        server = self.make_locker(self.server_file())
        self.assertTrue(server._write_fingerprint(server._serverPort))
        os.chmod(server.serverFile, 0o660)
        self.assertFalse(server._write_fingerprint(server._serverPort))


class TestLostConnectionCode(LockerTestCase):
    """Checks the error code of a lock call whose connection is gone."""

    def test_waiting_client_gets_code_one_when_the_server_stops(self):
        """Returns code 1, as documented, to a client that waited when its server stopped."""
        server, client = self.make_pair()
        path = self.lock_path('busy')
        server.acquire_lock(path, timeout=5)
        result = []
        import threading
        waiter = threading.Thread(target=lambda: result.append(client.acquire_lock(path, timeout=20, retries=0)))
        waiter.start()
        time.sleep(0.5)
        server.stop()
        waiter.join(10)
        self.assertEqual(result, [(False, 1)])


class TestFailedConnectAttempt(LockerTestCase):
    """Checks that a connection whose login failed is closed."""

    def test_connection_is_closed_after_a_failed_login(self):
        """Closes the connection of every attempt that failed after the TCP connection was open."""
        server = self.make_locker(self.server_file())
        client = self.make_client(server.serverFile, autoconnect=False)
        address, port = self.server_address(client)
        opened = []

        async def break_login(self, reader, writer):
            opened.append(writer)
            raise RuntimeError('login broke')
        with mock.patch.object(ServerLocker, '_client_login', break_login):
            client.start(address, port, connectAttempts=2)
        self.assertFalse(client.isClient)
        self.assertEqual(len(opened), 2)
        self.assertTrue(all(writer.is_closing() for writer in opened))


class TestPublishTimeout(LockerTestCase):
    """Checks that a publication timeout the server would drop is refused at once."""

    def test_unusable_timeouts_are_refused(self):
        """Raises ValueError for a boolean, infinite or not-a-number timeout."""
        server = self.make_locker(self.server_file())
        for timeout in (True, float('inf'), float('nan'), 0, -1):
            with self.assertRaises(ValueError, msg=repr(timeout)):
                server.publish_message('hello', timeout=timeout)
        self.assertEqual(server.publish_message('hello', timeout=5)[0], True)


class TestSingleLockerThreads(unittest.TestCase):
    """Checks that threads which create the SingleLocker together get the same one."""

    def test_threads_get_one_locker(self):
        """Gives the same object to 30 threads that create it at the same moment."""
        code = """
        import threading
        from pylocker import SingleLocker
        barrier = threading.Barrier(30)
        found = []
        def create():
            barrier.wait()
            found.append(SingleLocker('pw', serverFile=None, autoconnect=False))
        threads = [threading.Thread(target=create) for _ in range(30)]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
        print(len({id(locker) for locker in found}))
        """
        completed = run_child(code)
        self.assertEqual(completed.stdout.strip().splitlines()[-1], '1', completed.stderr[-300:])


if __name__ == '__main__':
    unittest.main()

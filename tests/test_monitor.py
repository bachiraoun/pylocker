"""Tests the server's monitor that frees the locks of clients whose process has died."""
import os
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, dead_process_id, spawn_child, wait_until


class FakeWriter:
    """A stand-in for a client connection that only remembers whether it was closed."""

    def __init__(self):
        """Creates an open connection."""
        self.closed = False

    def close(self):
        """Closes the connection."""
        self.closed = True

    def is_closing(self):
        """Tells whether the connection was closed.

        :Returns:
            #. isClosing (bool): True once close was called.
        """
        return self.closed


class TestDeadClientMonitor(LockerTestCase):
    """Checks which clients the monitor frees and which it leaves alone."""

    def run_scenario(self, psutilValue):
        """Registers a dead, a live, a junk-pid and a remote client on a server and lets the monitor run.

        :Parameters:
            #. psutilValue (module, None): The psutil module to use, or None for the fallback check.

        :Returns:
            #. server (ServerLocker): The server after the monitor ran.
            #. writers (dict): The fake connections by client name.
        """
        with mock.patch.object(lockerModule, 'psutil', psutilValue), \
                mock.patch.object(lockerModule, '_DEAD_PID_POLL', 0.3):
            server = self.make_locker(self.server_file())
            writers = {name: FakeWriter() for name in ('dead', 'live', 'junk', 'far')}
            clients = {'dead': dead_process_id(), 'live': os.getpid(), 'junk': 'abc', 'far': 999999}
            for name, pid in clients.items():
                address = '10.9.9.9' if name == 'far' else '127.0.0.1'
                server._clientsLUT[name] = {'writer': writers[name], 'pid': pid, 'address': address, 'name': name}
            for name in ('dead', 'live'):
                server._pathsLUT[f'path-{name}'] = {'request_unique_id': name, '_writer': writers[name],
                                                    'acquired_utctime': time.time(), 'path': f'path-{name}',
                                                    'client_unique_name': name}
            isFreed = wait_until(lambda: 'path-dead' not in server._pathsLUT, seconds=8)
            self.assertTrue(isFreed, 'the dead client lock was not freed')
            time.sleep(1.2)
        return server, writers

    def check_monitor(self, psutilValue):
        """Checks the monitor with a given way of testing whether a process is alive.

        :Parameters:
            #. psutilValue (module, None): The psutil module to use, or None for the fallback check.
        """
        server, writers = self.run_scenario(psutilValue)
        self.assertTrue(writers['dead'].closed)
        self.assertIn('path-live', server._pathsLUT)
        self.assertFalse(writers['live'].closed)
        for name in ('live', 'junk', 'far'):
            self.assertIn(name, server._clientsLUT)
        self.assertFalse(server._stopEvent.is_set())
        self.assertTrue(all(not task.done() for task in server._bgTasks))

    def test_with_psutil(self):
        """Frees the dead client and leaves the others alone, using psutil."""
        if lockerModule.psutil is None:
            self.skipTest('psutil is not installed')
        self.check_monitor(lockerModule.psutil)

    def test_without_psutil(self):
        """Frees the dead client and leaves the others alone, using the fallback check."""
        self.check_monitor(None)


class TestKilledClient(LockerTestCase):
    """Checks that the lock of a client killed in the middle of work is freed."""

    def test_lock_of_a_killed_client_is_freed(self):
        """Grants the path to another locker soon after its holder is killed."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        code = f"""
        client = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5, allowServing=False)
        print(client.acquire_lock('held', timeout=5)[0], flush=True)
        time.sleep(60)
        """
        child = spawn_child(code)
        self.addCleanup(child.kill)
        self.assertEqual(child.stdout.readline().strip(), 'True')
        child.kill()
        child.wait()
        start = time.time()
        acquired, _ = server.acquire_lock('held', timeout=10, retries=0)
        self.assertTrue(acquired)
        self.assertLess(time.time() - start, 6)


if __name__ == '__main__':
    unittest.main()

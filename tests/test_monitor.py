"""Tests the server's monitor that frees the locks of clients whose process has died."""
import json
import os
import socket
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, dead_process_id, login_by_hand, spawn_child, wait_until


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


class TestClockStep(LockerTestCase):
    """Checks that a step of the system clock changes neither how long a lock is held nor how long a request waits."""

    def poll_monitor_often(self):
        """Makes the monitor that frees old locks wake every 0.1 s until the test ends."""
        patcher = mock.patch.object(lockerModule, '_MONITOR_POLL', 0.1)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_lock_is_kept_when_the_clock_steps_forward(self):
        """Keeps a lock that was held for a moment while the clock jumps far ahead of the maximum lock time."""
        self.poll_monitor_often()
        server, client = self.make_pair(maxLockTime=100)
        acquired, _ = client.acquire_lock('held', timeout=2, retries=0)
        self.assertTrue(acquired)
        realTime = time.time
        with mock.patch('time.time', lambda: realTime() + 500):
            time.sleep(0.6)
        self.assertIn('held', server.lockedPaths)

    def test_lock_is_still_freed_after_its_real_time(self):
        """Frees a lock once it has been held for the maximum lock time."""
        self.poll_monitor_often()
        server, client = self.make_pair(maxLockTime=1)
        acquired, _ = client.acquire_lock('held', timeout=2, retries=0)
        self.assertTrue(acquired)
        self.assertTrue(wait_until(lambda: 'held' not in server.lockedPaths, seconds=5))

    def test_waiting_request_does_not_expire_when_the_clock_steps_forward(self):
        """Keeps a request waiting for its whole timeout when the clock jumps ahead while it waits."""
        server, client = self.make_pair()
        acquired, _ = client.acquire_lock('held', timeout=2, retries=0)
        self.assertTrue(acquired)
        realTime = time.time

        def step_clock():
            """Moves the clock ahead for a short while."""
            time.sleep(0.4)
            with mock.patch('time.time', lambda: realTime() + 500):
                time.sleep(0.5)

        thread = threading.Thread(target=step_clock)
        thread.start()
        started = time.time()
        result = server.acquire_lock('held', timeout=2.5, retries=0)
        thread.join()
        self.assertEqual(result, (False, 0))
        self.assertGreater(time.time() - started, 2.0)


    def test_request_is_dropped_at_its_timeout(self):
        """Never grants a request after its timeout has passed, even when the lock is freed later."""
        server, holder = self.make_pair()
        acquired, lockId = holder.acquire_lock('held', timeout=2, retries=0)
        self.assertTrue(acquired)
        raw = socket.create_connection(('127.0.0.1', server._serverPort), timeout=1)
        reader = raw.makefile('rb')
        self.addCleanup(raw.close)
        self.addCleanup(reader.close)
        self.assertEqual(login_by_hand(raw, reader, uniqueName='raw')['action'], 'welcome')
        raw.sendall(json.dumps({'action': 'acquire', 'request_unique_id': 'late-1', 'path': ['held'],
                                'timeout': 1}).encode('utf-8') + b'\n')
        time.sleep(1.5)
        holder.release_lock(lockId)
        with self.assertRaises(socket.timeout):
            reader.readline()
        self.assertNotIn('held', server.lockedPaths)


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

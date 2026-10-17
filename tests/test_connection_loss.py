"""Tests what callers see when the connection to the server is lost, and what a stopped server leaves behind."""
import asyncio
import os
import random
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase


class TestRetryAfterLostConnection(LockerTestCase):
    """Checks that a waiting caller is told at once when the server goes away between two attempts."""

    def test_retry_after_the_server_stopped_reports_a_lost_connection(self):
        """Returns code 1 at the next attempt instead of waiting for the whole timeout again."""
        server, client = self.make_pair()
        acquired, _ = server.acquire_lock('held', timeout=2, retries=0)
        self.assertTrue(acquired)
        timer = threading.Timer(1.5, server.stop)
        timer.start()
        self.addCleanup(timer.cancel)
        started = time.time()
        result = client.acquire_lock('held', timeout=1, retries=2, retryDelay=2)
        self.assertEqual(result, (False, 1))
        # First attempt ends at 1 s, the pause ends at 3 s, and the second attempt must fail at once
        self.assertLess(time.time() - started, 4.5)

    def test_acquire_without_a_connection_raises_connection_error_inside(self):
        """Fails the acquire coroutine at once when there is no connection."""
        _, client = self.make_pair()
        client._writer = None
        with self.assertRaises(ConnectionError):
            client._run_sync(client._client_acquire('id', ['x'], 5), timeout=2)
        self.assertEqual(client._pending, {})


class TestConnectGivesUp(LockerTestCase):
    """Checks that a connect() that gave up does not connect later."""

    def test_timed_out_connect_stops_trying(self):
        """Leaves the locker unconnected after connect() returned False."""
        server = self.make_locker(self.server_file())
        client = self.make_client(server.serverFile, autoconnect=False, connectTimeout=1)
        realLogin = client._client_login
        calls = []

        async def slow_login(reader, writer):
            """Fails twice after a long wait, as on a network that drops packets, then works."""
            calls.append(time.time())
            if len(calls) < 3:
                await asyncio.sleep(1.2)
                raise ConnectionError('simulated slow network')
            return await realLogin(reader, writer)

        client._client_login = slow_login
        client._start_loop()
        client._run_sync(client._init_async_state())
        address, port = server.address, server._serverPort
        self.assertFalse(client.connect(address, port, connectAttempts=3))
        time.sleep(2.5)
        self.assertFalse(client.isClient)
        self.assertEqual(len(calls), 2)


class TestStopLeavesNothingBehind(LockerTestCase):
    """Checks that the server file is empty after a server stops, even when the heartbeat is busy."""

    def test_heartbeat_cannot_put_the_fingerprint_back(self):
        """Finds the server file empty after each of many stops at random moments."""
        serverFile = self.server_file()
        leftOver = 0
        with mock.patch.object(lockerModule, '_HEARTBEAT_INTERVAL', 0.003):
            for _ in range(120):
                server = self.make_locker(serverFile)
                time.sleep(random.uniform(0.0, 0.02))
                server.stop()
                time.sleep(0.02)
                if os.path.getsize(serverFile) > 0:
                    leftOver += 1
        self.assertEqual(leftOver, 0)

    def test_server_can_start_again_after_stop(self):
        """Writes its fingerprint again when a stopped server is started once more."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        server.stop()
        server.start()
        self.assertTrue(server.isServer)
        self.assertEqual(server.get_running_server_fingerprint()[0], server.uniqueName)


if __name__ == '__main__':
    unittest.main()

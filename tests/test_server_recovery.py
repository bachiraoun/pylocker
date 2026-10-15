"""Tests the server's heartbeat and what happens to its clients and to the next election when the server process dies."""
import os
import signal
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, spawn_child, wait_until

SERVER_PROCESS = """
import pylocker.Locker as lockerModule
lockerModule._HEARTBEAT_INTERVAL = 0.3
locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)
print('UP', flush=True)
time.sleep(120)
"""


class TestHeartbeat(LockerTestCase):
    """Checks that a living server keeps its entry in the server file fresh."""

    def read_timestamp(self, locker):
        """Reads the time the server last wrote in the server file.

        :Parameters:
            #. locker (ServerLocker): Any locker that uses the server file.

        :Returns:
            #. timestamp (float): The seconds since the epoch written by the server.
        """
        return float(locker.get_running_server_fingerprint(serverFile=locker.serverFile)[1])

    def test_timestamp_moves_forward(self):
        """Rewrites the server file at every heartbeat."""
        with mock.patch.object(lockerModule, '_HEARTBEAT_INTERVAL', 0.3):
            server = self.make_locker(self.server_file())
            first = self.read_timestamp(server)
            time.sleep(1.0)
            second = self.read_timestamp(server)
        self.assertGreater(second, first)

    def test_fresh_entry_is_not_taken_over(self):
        """Lets a new locker join a living server instead of taking its place."""
        with mock.patch.object(lockerModule, '_HEARTBEAT_INTERVAL', 0.3):
            serverFile = self.server_file()
            server = self.make_locker(serverFile)
            time.sleep(2.5)
            late = self.make_locker(serverFile)
        self.assertTrue(server.isServer)
        self.assertTrue(late.isClient)

    def test_stopped_server_clears_its_entry(self):
        """Empties the server file when the server stops."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        server.stop()
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read().strip(), '')


class TestKilledServer(LockerTestCase):
    """Checks recovery after the process of a server is killed."""

    def start_server_process(self, serverFile):
        """Starts a server in another process and waits until it serves.

        :Parameters:
            #. serverFile (str): The server file path.

        :Returns:
            #. process (subprocess.Popen): The server process.
        """
        child = spawn_child(SERVER_PROCESS.format(serverFile=serverFile))
        self.addCleanup(child.kill)
        self.assertEqual(child.stdout.readline().strip(), 'UP')
        return child

    def test_client_notices_and_a_new_server_takes_over(self):
        """Fails the client's lock calls, then elects a new server once the old entry is stale."""
        serverFile = self.server_file()
        child = self.start_server_process(serverFile)
        client = self.make_client(serverFile)
        self.assertTrue(client.acquire_lock(self.lock_path('k1'), timeout=2)[0])
        child.kill()
        child.wait()
        self.assertTrue(wait_until(lambda: not client.isClient, seconds=5), 'the client did not notice the loss')
        acquired, code = client.acquire_lock(self.lock_path('k2'), timeout=1, retries=0)
        self.assertFalse(acquired)
        self.assertEqual(code, 2)
        start = time.time()
        newServer = self.make_locker(serverFile, electionTimeout=20)
        self.assertTrue(newServer.isServer)
        self.assertTrue(1.0 < time.time() - start < 10)

    def test_waiting_request_fails_when_the_server_dies(self):
        """Returns a connection-lost error to a caller that was waiting for a path when the server died."""
        serverFile = self.server_file()
        child = self.start_server_process(serverFile)
        holder = self.make_client(serverFile)
        waiter = self.make_client(serverFile)
        path = self.lock_path('contended')
        self.assertTrue(holder.acquire_lock(path, timeout=2)[0])
        result = {}

        def wait():
            """Waits for the held path and keeps the answer."""
            result['answer'] = waiter.acquire_lock(path, timeout=30, retries=0)

        thread = threading.Thread(target=wait)
        thread.start()
        time.sleep(0.5)
        child.kill()
        child.wait()
        thread.join(10)
        self.assertFalse(thread.is_alive(), 'the waiting call did not return')
        self.assertEqual(result['answer'], (False, 'Server connection lost'))

    def test_new_server_starts_with_no_locks(self):
        """Forgets the locks the dead server's clients held."""
        serverFile = self.server_file()
        child = self.start_server_process(serverFile)
        client = self.make_client(serverFile)
        path = self.lock_path('held')
        self.assertTrue(client.acquire_lock(path, timeout=2)[0])
        child.kill()
        child.wait()
        newServer = self.make_locker(serverFile, electionTimeout=20)
        self.assertEqual(newServer.lockedPaths, {})
        self.assertTrue(newServer.acquire_lock(path, timeout=2)[0])

    def test_old_client_can_join_the_new_server(self):
        """Lets the client of a dead server start again and connect to its successor."""
        serverFile = self.server_file()
        child = self.start_server_process(serverFile)
        client = self.make_client(serverFile)
        child.kill()
        child.wait()
        client.acquire_lock(self.lock_path('probe'), timeout=1, retries=0)
        newServer = self.make_locker(serverFile, electionTimeout=20)
        self.assertTrue(newServer.isServer)
        client.start()
        self.assertTrue(client.isClient)
        self.assertTrue(client.acquire_lock(self.lock_path('again'), timeout=2)[0])

    def test_client_killed_while_server_lives(self):
        """Keeps serving after a client process is killed."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        code = f"""
        client = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5, allowServing=False)
        print(client.acquire_lock({self.lock_path('c')!r}, timeout=3)[0], flush=True)
        time.sleep(60)
        """
        child = spawn_child(code)
        self.addCleanup(child.kill)
        self.assertEqual(child.stdout.readline().strip(), 'True')
        os.kill(child.pid, signal.SIGKILL)
        child.wait()
        self.assertTrue(server.isServer)
        self.assertTrue(server.acquire_lock(self.lock_path('c'), timeout=10, retries=0)[0])


if __name__ == '__main__':
    unittest.main()

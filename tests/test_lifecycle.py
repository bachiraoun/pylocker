"""Tests starting, stopping and restarting lockers, and what happens to locks when a locker or its connection ends."""
import threading
import time

from pylocker import ServerLocker

from helpers import LockerTestCase, PASSWORD, run_child, wait_until


class TestIdleLocker(LockerTestCase):
    """Checks a locker that was never started."""

    def test_idle_locker_has_no_connection(self):
        """Is neither server nor client, and has no event loop."""
        locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False)
        self.assertFalse(locker.isServer)
        self.assertFalse(locker.isClient)
        self.assertIsNone(locker._loop)
        self.assertEqual(locker._bgTasks, [])


class TestStopClient(LockerTestCase):
    """Checks what stopping a client does."""

    def test_stop_releases_the_locks_of_the_client(self):
        """Gives the paths of a stopped client back to the server."""
        server, client = self.make_pair()
        path = self.lock_path('x1')
        self.assertTrue(client.acquire_lock(path, timeout=2)[0])
        self.assertEqual(server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        client.stop()
        self.assertFalse(client.isClient)
        self.assertIsNone(client._writer)
        self.assertTrue(server.acquire_lock(path, timeout=3)[0])

    def test_stop_twice_is_harmless(self):
        """Does nothing the second time."""
        _, client = self.make_pair()
        client.stop()
        client.stop()
        self.assertFalse(client.isClient)

    def test_restart_after_stop(self):
        """Connects again and locks after a stop."""
        _, client = self.make_pair()
        client.stop()
        client.start()
        self.assertTrue(client.isClient)
        self.assertTrue(client.acquire_lock(self.lock_path('r1'), timeout=2)[0])

    def test_private_loop_client(self):
        """Locks with its own event loop and closes that loop on stop."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        client = self.make_client(serverFile, sharedLoop=False)
        path = self.lock_path('pl1')
        acquired, _ = client.acquire_lock(path, timeout=2)
        self.assertTrue(client.isClient and acquired)
        self.assertFalse(client._usesSharedLoop)
        thread = client._loopThread
        client.stop()
        self.assertTrue(thread is None or not thread.is_alive())
        self.assertTrue(server.acquire_lock(path, timeout=3)[0])

    def test_many_clients_stop_together(self):
        """Frees the locks of eight clients that stop at the same moment."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        clients = [self.make_client(serverFile) for _ in range(8)]
        self.assertTrue(all(client.acquire_lock(self.lock_path(f'm{index}'), timeout=2)[0]
                            for index, client in enumerate(clients)))
        threads = [threading.Thread(target=client.stop) for client in clients]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertTrue(all(not client.isClient for client in clients))
        self.assertTrue(all(server.acquire_lock(self.lock_path(f'm{index}'), timeout=3)[0] for index in range(8)))


class TestStopServer(LockerTestCase):
    """Checks what stopping a server does."""

    def test_server_stop_makes_it_idle(self):
        """Clears the state of the server."""
        server = self.make_locker(self.server_file())
        server.stop()
        self.assertFalse(server.isServer)
        self.assertIsNone(server._pathsLUT)
        self.assertIsNone(server._server)

    def test_client_notices_the_loss(self):
        """Fails to lock once the server is gone."""
        server, client = self.make_pair()
        server.stop()
        time.sleep(0.5)
        self.assertFalse(client.acquire_lock(self.lock_path('q'), timeout=0.5, retries=0)[0])

    def test_new_server_is_elected_after_the_old_one_stopped(self):
        """Lets a new locker become the server after the old server stopped."""
        serverFile = self.server_file()
        old = self.make_locker(serverFile)
        old.stop()
        new = self.make_locker(serverFile)
        self.assertTrue(new.isServer)


class TestQuietShutdown(LockerTestCase):
    """Checks that stopping a server that still has clients leaves nothing behind."""

    def test_server_stop_with_clients_prints_no_warnings(self):
        """Prints no error output when a server stops while clients are connected and the process then exits."""
        serverFile = self.server_file()
        code = f"""
        import warnings
        warnings.simplefilter('default')
        server = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)
        clients = [ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5, allowServing=False)
                   for _ in range(3)]
        print(all(client.isClient for client in clients), flush=True)
        server.stop()
        """
        completed = run_child(code)
        self.assertEqual(completed.stdout.strip(), 'True')
        self.assertEqual(completed.stderr.strip(), '')

    def test_stop_does_not_wait_for_ever_for_a_stuck_client_task(self):
        """Returns from stop within a few seconds even when a client's task ignores the request to end."""
        serverFile = self.server_file()
        code = f"""
        import asyncio
        import pylocker.Locker as lockerModule
        lockerModule._CLIENT_STOP_TIMEOUT = 0.5
        original = ServerLocker._read_message_timeout

        async def stubborn(locker, reader, timeout):
            try:
                return await original(locker, reader, timeout)
            except asyncio.CancelledError:
                await asyncio.sleep(30)

        ServerLocker._read_message_timeout = stubborn
        server = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)
        client = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5, allowServing=False)
        start = time.time()
        server.stop()
        print('stopped', round(time.time() - start, 1), flush=True)
        os._exit(0)
        """
        completed = run_child(code, timeout=30)
        self.assertTrue(completed.stdout.strip().startswith('stopped'), completed.stderr[-400:])
        self.assertLess(float(completed.stdout.split()[1]), 4)

    def test_clients_notice_the_server_stop_without_hanging_it(self):
        """Stops a server quickly even with several clients connected."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        clients = [self.make_client(serverFile) for _ in range(5)]
        start = time.time()
        server.stop()
        self.assertLess(time.time() - start, 3)
        self.assertTrue(all(wait_until(lambda client=client: not client.isClient) for client in clients))


class TestDeadConnections(LockerTestCase):
    """Checks that locks are freed when a client's connection breaks."""

    def abort_connection(self, client):
        """Breaks the connection of a client without a clean goodbye.

        :Parameters:
            #. client (ServerLocker): The connected client.
        """
        client._loop.call_soon_threadsafe(client._writer.transport.abort)

    def test_abrupt_disconnect_frees_the_locks(self):
        """Frees the locks of a client whose connection was cut, and forgets the client."""
        server, client = self.make_pair()
        path = self.lock_path('d2')
        self.assertTrue(client.acquire_lock(path, timeout=2)[0])
        self.abort_connection(client)
        self.assertTrue(server.acquire_lock(path, timeout=5)[0])
        self.assertTrue(wait_until(lambda: len(server._clientsLUT) == 0))

    def test_queued_request_of_a_vanished_client_is_not_granted(self):
        """Does not give a path to a client that left while it waited for it."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        vanishing = self.make_client(serverFile)
        other = self.make_client(serverFile)
        path = self.lock_path('res')
        acquired, holdId = server.acquire_lock(path, timeout=5)
        self.assertTrue(acquired)
        waiter = threading.Thread(target=lambda: vanishing.acquire_lock(path, timeout=20), daemon=True)
        waiter.start()
        time.sleep(0.5)
        self.abort_connection(vanishing)
        time.sleep(0.5)
        server.release_lock(holdId)
        time.sleep(0.3)
        start = time.time()
        acquired, _ = other.acquire_lock(path, timeout=4, retries=0)
        self.assertTrue(acquired)
        self.assertLess(time.time() - start, 3)

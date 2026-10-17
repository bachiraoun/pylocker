"""Tests the less common ways a locker starts: no server file, explicit addresses, a lost election, blocking, ports."""
import asyncio
import logging
import os
import socket
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule
from pylocker import AuthenticationError, ServerLocker

from helpers import LockerTestCase, PASSWORD, dead_process_id, spawn_child, start_sleeper, wait_until


class TestWithoutServerFile(LockerTestCase):
    """Checks lockers that have no server file to find each other by."""

    def test_serves_without_a_file(self):
        """Serves when it may serve, and a client joins it by address and port."""
        server = self.make_locker(False)
        self.assertTrue(server.isServer)
        client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False))
        client.start(address='127.0.0.1', port=server._serverPort)
        self.assertTrue(client.isClient)
        self.assertTrue(client.acquire_lock(self.lock_path('nf'), timeout=2)[0])

    def test_cannot_discover_a_server_without_a_file(self):
        """Stays idle and warns when it may not serve and has no file to look in."""
        logger = logging.getLogger('pylocker.test.nofile')
        with self.assertLogs(logger, level='WARNING') as captured:
            locker = self.make_locker(False, allowServing=False, logger=logger)
        self.assertFalse(locker.isServer or locker.isClient)
        self.assertTrue(any('cannot auto-discover' in line for line in captured.output))


class TestExplicitAddress(LockerTestCase):
    """Checks start with an address and a port."""

    def setUp(self):
        """Creates a server."""
        super().setUp()
        self.server = self.make_locker(self.server_file())
        self.address, self.port = self.server_address(self.server)

    def idle_client(self, **kwargs):
        """Creates a client that does not start.

        :Parameters:
            #. kwargs (dict): Arguments of ``ServerLocker``, replacing the defaults.

        :Returns:
            #. client (ServerLocker): The idle client.
        """
        arguments = {'password': PASSWORD, 'autoconnect': False, 'serverFile': False, 'allowServing': False}
        arguments.update(kwargs)
        return self.track(ServerLocker(**arguments))

    def test_start_with_address_and_port(self):
        """Connects to the given address and port."""
        client = self.idle_client()
        client.start(address=self.address, port=self.port)
        self.assertTrue(client.isClient)

    def test_start_with_a_password_for_this_call(self):
        """Uses the password given to start instead of the one of the locker."""
        client = self.idle_client(password='wrong')
        client.start(address=self.address, port=self.port, password=PASSWORD)
        self.assertTrue(client.isClient)

    def test_wrong_password_in_start_is_refused(self):
        """Raises AuthenticationError when the password given to start is wrong."""
        client = self.idle_client()
        with self.assertRaises(AuthenticationError):
            client.start(address=self.address, port=self.port, password='nope')

    def test_nothing_listens_at_the_address(self):
        """Stays idle when the port has no server."""
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            freePort = probe.getsockname()[1]
        client = self.idle_client(connectTimeout=2)
        client.start(address='127.0.0.1', port=freePort, connectAttempts=1)
        self.assertFalse(client.isClient)

    def test_start_twice_is_harmless(self):
        """Warns and does nothing when a client or a server is started again."""
        client = self.idle_client()
        client.start(address=self.address, port=self.port)
        client.start(address=self.address, port=self.port)
        self.assertTrue(client.isClient)
        self.server.start()
        self.assertTrue(self.server.isServer)

    def test_connect_to_a_dead_port_returns_false(self):
        """Returns False from connect when the connection cannot be made."""
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            freePort = probe.getsockname()[1]
        client = self.idle_client(connectTimeout=2)
        client._start_loop()
        client._run_sync(client._init_async_state())
        self.assertFalse(client.connect('127.0.0.1', freePort, connectAttempts=1))


class TestLostElection(LockerTestCase):
    """Checks what a locker does when it finds out it did not win the election."""

    def locker_for(self, serverFile, **kwargs):
        """Creates a locker that is ready to elect but has not started.

        :Parameters:
            #. serverFile (str): The server file path.
            #. kwargs (dict): Arguments of ``ServerLocker``, replacing the defaults.

        :Returns:
            #. locker (ServerLocker): The idle locker with its event loop running.
        """
        arguments = {'password': PASSWORD, 'serverFile': serverFile, 'autoconnect': False, 'connectTimeout': 3}
        arguments.update(kwargs)
        locker = self.track(ServerLocker(**arguments))
        locker._start_loop()
        locker._run_sync(locker._init_async_state())
        return locker

    def test_waiting_loser_connects_to_the_winner(self):
        """Connects to a winner that has published its port."""
        serverFile = self.server_file()
        self.make_locker(serverFile)
        loser = self.locker_for(serverFile, allowServing=False)
        self.assertTrue(loser._wait_for_winner(connectAttempts=1, electionTimeout=3))
        self.assertTrue(loser.isClient)

    def test_loser_waits_for_a_port_that_appears_late(self):
        """Keeps waiting while the winner has no port yet, and connects once it appears."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        address, port = self.server_address(server)
        loser = self.locker_for(serverFile, allowServing=False)
        fingerprint = server.get_running_server_fingerprint(serverFile=serverFile)
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write(f'{fingerprint[0]}({time.time()})@{address}:PORT[{fingerprint[4]}]\n')

        def publish_port():
            """Writes the real port after a short delay."""
            time.sleep(0.5)
            with open(serverFile, 'w') as fileHandle:
                fileHandle.write(f'{fingerprint[0]}({time.time()})@{address}:{port}[{fingerprint[4]}]\n')

        thread = threading.Thread(target=publish_port)
        thread.start()
        self.assertTrue(loser._wait_for_winner(connectAttempts=1, electionTimeout=5))
        thread.join()

    def test_stale_winner_is_given_up_on(self):
        """Stops waiting for a winner whose entry is stale."""
        serverFile = self.server_file()
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write(f'ghost({time.time() - 30})@127.0.0.1:PORT[1]\n')
        loser = self.locker_for(serverFile)
        start = time.time()
        self.assertFalse(loser._wait_for_winner(connectAttempts=1, electionTimeout=10))
        self.assertLess(time.time() - start, 2)

    def test_claim_of_a_dead_process_is_given_up_on(self):
        """Stops waiting at once for a claim without a port whose process is gone."""
        serverFile = self.server_file()
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write(f'ghost(TIMESTAMP)@127.0.0.1:PORT[{dead_process_id()}]\n')
        loser = self.locker_for(serverFile)
        start = time.time()
        self.assertFalse(loser._wait_for_winner(connectAttempts=1, electionTimeout=10))
        self.assertLess(time.time() - start, 1.5)

    def test_old_claim_of_a_live_process_is_given_up_on(self):
        """Stops waiting for a claim without a port that is older than the stale limit."""
        owner = start_sleeper()
        self.addCleanup(owner.kill)
        serverFile = self.server_file()
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write(f'ghost(TIMESTAMP)@127.0.0.1:PORT[{owner.pid}]\n')
        past = time.time() - 30
        os.utime(serverFile, (past, past))
        loser = self.locker_for(serverFile)
        start = time.time()
        self.assertFalse(loser._wait_for_winner(connectAttempts=1, electionTimeout=10))
        self.assertLess(time.time() - start, 1.5)

    def test_fresh_claim_of_a_live_process_is_waited_for(self):
        """Keeps waiting for a claim without a port while its process lives, and connects when the port appears."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        address, port = self.server_address(server)
        fingerprint = server.get_running_server_fingerprint(serverFile=serverFile)
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write(f'{fingerprint[0]}(TIMESTAMP)@{address}:PORT[{os.getpid()}]\n')
        loser = self.locker_for(serverFile, allowServing=False)

        def publish_port():
            """Writes the real port after a short delay."""
            time.sleep(0.5)
            with open(serverFile, 'w') as fileHandle:
                fileHandle.write(f'{fingerprint[0]}({time.time()})@{address}:{port}[{os.getpid()}]\n')

        thread = threading.Thread(target=publish_port)
        thread.start()
        self.assertTrue(loser._wait_for_winner(connectAttempts=1, electionTimeout=5))
        thread.join()

    def test_wait_times_out_when_nothing_happens(self):
        """Gives up after the election timeout when the file stays empty."""
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        loser = self.locker_for(serverFile)
        start = time.time()
        self.assertFalse(loser._wait_for_winner(connectAttempts=1, electionTimeout=0.5))
        self.assertTrue(0.4 < time.time() - start < 2)


class TestServerStartFailure(LockerTestCase):
    """Checks that a server that cannot start leaves a clean server file."""

    def test_failed_start_empties_the_file(self):
        """Empties the server file and does not serve when the server cannot be started."""
        serverFile = self.server_file()
        logger = logging.getLogger('pylocker.test.startfail')

        async def fail(locker):
            """Stands in for a server start that fails."""
            raise OSError('no socket for you')

        with mock.patch.object(ServerLocker, '_start_server', fail):
            with self.assertLogs(logger, level='ERROR') as captured:
                locker = self.make_locker(serverFile, logger=logger, electionTimeout=5)
        self.assertFalse(locker.isServer)
        self.assertTrue(any('failed to start server' in line for line in captured.output))
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read().strip(), '')


class TestPortChoice(LockerTestCase):
    """Checks how a server picks its port."""

    def test_busy_port_is_skipped(self):
        """Binds the next free port when the preferred one is taken."""
        with socket.socket() as blocker:
            blocker.bind(('127.0.0.1', 0))
            blocker.listen(1)
            taken = blocker.getsockname()[1]
            server = self.make_locker(self.server_file(), port=taken)
            self.assertTrue(server.isServer)
            self.assertNotEqual(self.server_address(server)[1], taken)
            client = self.make_client(self.server_file())
            self.assertTrue(client.isClient)


class TestBlockingServer(LockerTestCase):
    """Checks a server that keeps its starting call open until it stops."""

    def test_blocking_server_returns_when_stopped(self):
        """Keeps the creating call open while serving and ends the process when a stop order arrives."""
        serverFile = self.server_file()
        code = f"""
        import threading
        threading.Timer(0.5, lambda: print('SERVING', flush=True)).start()
        locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5, blocking=True,
                              allowRemoteOrders=True)
        print('RETURNED', flush=True)
        """
        child = spawn_child(code)
        self.addCleanup(child.kill)
        self.assertEqual(child.stdout.readline().strip(), 'SERVING')
        self.assertIsNone(child.poll())
        client = self.make_client(serverFile)
        self.assertTrue(client.isClient)
        client.stop_remote(password=PASSWORD)
        self.assertEqual(child.stdout.readline().strip(), 'RETURNED')
        self.assertTrue(wait_until(lambda: child.poll() is not None, seconds=10))


class TestFingerprintReading(LockerTestCase):
    """Checks reading the server file."""

    def setUp(self):
        """Creates an idle locker."""
        super().setUp()
        self.locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False)

    def test_missing_file(self):
        """Returns empty fields, or raises FileNotFoundError when asked to."""
        missing = self.lock_path('missing.lock')
        self.assertEqual(self.locker.get_running_server_fingerprint(serverFile=missing), (None,) * 5)
        with self.assertRaises(FileNotFoundError):
            self.locker.get_running_server_fingerprint(serverFile=missing, raiseNotFound=True)

    def test_empty_file(self):
        """Returns empty fields for an empty file."""
        empty = self.lock_path('empty.lock')
        open(empty, 'w').close()
        self.assertEqual(self.locker.get_running_server_fingerprint(serverFile=empty), (None,) * 5)

    def test_garbage_file(self):
        """Returns empty fields for a file that is no fingerprint, so the election treats it as vacant."""
        garbage = self.lock_path('garbage.lock')
        with open(garbage, 'w') as fileHandle:
            fileHandle.write('not a fingerprint\n')
        self.assertEqual(self.locker.get_running_server_fingerprint(serverFile=garbage), (None,) * 5)

    @unittest.skipIf(hasattr(os, 'geteuid') and os.geteuid() == 0, 'a superuser can read every file')
    def test_unreadable_file(self):
        """Raises when the file cannot be read, or returns empty fields when told not to."""
        locked = self.lock_path('locked.lock')
        with open(locked, 'w') as fileHandle:
            fileHandle.write('someone(1.0)@10.0.0.7:4000[77]\n')
        os.chmod(locked, 0)
        self.addCleanup(os.chmod, locked, 0o600)
        with self.assertRaises(PermissionError):
            self.locker.get_running_server_fingerprint(serverFile=locked)
        self.assertEqual(self.locker.get_running_server_fingerprint(serverFile=locked, raiseError=False), (None,) * 5)

    def test_good_file(self):
        """Splits a fingerprint into its five fields."""
        good = self.lock_path('good.lock')
        with open(good, 'w') as fileHandle:
            fileHandle.write('someone(123.5)@10.0.0.7:4000[77]\n')
        self.assertEqual(self.locker.get_running_server_fingerprint(serverFile=good),
                         ('someone', '123.5', '10.0.0.7', '4000', '77'))


class TestNetworkAddress(unittest.TestCase):
    """Checks how a locker finds its own network address."""

    def test_falls_back_to_loopback(self):
        """Returns the loopback address when the machine has no usable network."""
        with mock.patch.object(lockerModule.socket, 'socket', side_effect=OSError('no network')):
            self.assertEqual(lockerModule._get_ip(), '127.0.0.1')

    def test_returns_an_address_string(self):
        """Returns a dotted address."""
        self.assertEqual(len(lockerModule._get_ip().split('.')), 4)


class TestConnectWithoutLoop(LockerTestCase):
    """Checks connect on a locker that was never started."""

    def test_connect_returns_false_and_logs(self):
        """Returns False and logs an error when there is no event loop to connect with."""
        logger = logging.getLogger('pylocker.test.noloop')
        locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False, logger=logger)
        with self.assertLogs(logger, level='ERROR') as captured:
            self.assertFalse(locker.connect('127.0.0.1', 1, connectAttempts=1))
        self.assertTrue(any('connect() failed' in line for line in captured.output))

    def test_exit_hook_survives_failures(self):
        """Does not raise from the exit hook even when cleaning up fails."""
        server = self.make_locker(self.server_file())
        with mock.patch.object(ServerLocker, '_clear_fingerprint', side_effect=RuntimeError('boom')):
            with mock.patch.object(ServerLocker, 'stop', side_effect=RuntimeError('boom')):
                server._on_atexit()


class TestSetters(LockerTestCase):
    """Checks the setters that are not covered elsewhere."""

    def test_set_default_timeout(self):
        """Changes the timeout that acquire uses when none is given."""
        server, client = self.make_pair()
        client.set_default_timeout(0.4)
        self.assertEqual(client.defaultTimeout, 0.4)
        path = self.lock_path('dt')
        self.assertTrue(server.acquire_lock(path, timeout=2)[0])
        start = time.time()
        self.assertEqual(client.acquire_lock(path, retries=0), (False, 0))
        self.assertTrue(0.3 < time.time() - start < 1.5)

    def test_set_maximum_lock_time(self):
        """Changes the maximum lock time that is reported."""
        locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False)
        locker.set_maximum_lock_time(15)
        self.assertEqual(locker.maxLockTime, 15)

    def test_async_calls_on_a_locker_that_is_not_running(self):
        """Returns code 2 for an async acquire and code 3 for an async release of a known lock."""
        server = self.make_locker(self.server_file())
        acquired, lockId = server.acquire_lock(self.lock_path('x'), timeout=2)
        self.assertTrue(acquired)
        server.stop()
        self.assertEqual(asyncio.run(server.acquire_async(self.lock_path('y'), timeout=1)), (False, 2))
        self.assertEqual(asyncio.run(server.release_async(lockId)), (False, 3))
        self.assertEqual(asyncio.run(server.release_async('unknown')), (True, 0))


if __name__ == '__main__':
    unittest.main()

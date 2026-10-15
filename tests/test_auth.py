"""Tests the connection handshake: password check, malformed or silent connections, and local-only serving."""
import logging
import socket
import time
from unittest import mock

import pylocker
from pylocker import AuthenticationError, ServerLocker
from pylocker import Locker as lockerModule

from helpers import LockerTestCase


class TestPassword(LockerTestCase):
    """Checks that only a client with the right password joins."""

    def setUp(self):
        """Creates a server."""
        super().setUp()
        self.serverFile = self.server_file()
        self.server = self.make_locker(self.serverFile)

    def test_wrong_password_raises_at_once(self):
        """Raises AuthenticationError without waiting for the connect timeout."""
        start = time.time()
        with self.assertRaises(AuthenticationError):
            ServerLocker('WRONG', serverFile=self.serverFile, connectTimeout=5, allowServing=False)
        self.assertLess(time.time() - start, 2)

    def test_wrong_password_with_explicit_address(self):
        """Raises AuthenticationError when connecting with start(address, port)."""
        address, port = self.server_address(self.server)
        client = ServerLocker('WRONG', autoconnect=False, serverFile=False)
        with self.assertRaises(AuthenticationError):
            client.start(address=address, port=port)

    def test_right_password_still_connects(self):
        """Connects a client with the right password after a wrong one was refused."""
        with self.assertRaises(AuthenticationError):
            ServerLocker('WRONG', serverFile=self.serverFile, connectTimeout=5, allowServing=False)
        client = self.make_client(self.serverFile)
        self.assertTrue(client.isClient)
        self.assertTrue(self.server.isServer)

    def test_error_is_exported(self):
        """Exposes AuthenticationError at the package level."""
        self.assertIs(pylocker.AuthenticationError, AuthenticationError)


class TestRawConnections(LockerTestCase):
    """Checks how the server answers connections that do not follow the protocol."""

    def setUp(self):
        """Creates a server and finds its address."""
        super().setUp()
        self.server = self.make_locker(self.server_file())
        self.address, self.port = self.server_address(self.server)

    def talk(self, payload=None, wait=3.0):
        """Opens a plain connection, sends bytes if given, and returns what the server answers.

        :Parameters:
            #. payload (None, bytes): The bytes to send, or None to send nothing.
            #. wait (int, float): Seconds to wait for an answer.

        :Returns:
            #. answer (bytes): The reply, ``b''`` when the server closed the connection and ``b'TIMEOUT'`` when
               it neither answered nor closed.
        """
        with socket.create_connection((self.address, self.port), timeout=wait) as connection:
            if payload is not None:
                connection.sendall(payload)
            try:
                return connection.recv(4096)
            except socket.timeout:
                return b'TIMEOUT'

    def test_list_instead_of_hello_is_dropped(self):
        """Closes a connection whose first message is not an object."""
        self.assertEqual(self.talk(b'[1,2,3]\n'), b'')

    def test_wrong_password_gets_an_error_reply(self):
        """Answers a wrong password with a bad password error."""
        self.assertIn(b'bad password', self.talk(b'{"name":"x","unique_name":"u1","password":"WRONG"}\n'))

    def test_missing_password_counts_as_wrong(self):
        """Answers a hello without a password with a bad password error."""
        self.assertIn(b'bad password', self.talk(b'{"name":"x","unique_name":"u3"}\n'))

    def test_right_password_gets_a_welcome(self):
        """Answers a right password with a welcome that names the serializer."""
        answer = self.talk(b'{"name":"x","unique_name":"u2","password":"pw","pid":1,"address":"127.0.0.1"}\n')
        self.assertIn(b'welcome', answer)
        self.assertIn(b'"serializer"', answer)

    def test_server_survives_bad_connections(self):
        """Keeps serving after bad connections."""
        self.talk(b'not json at all\n')
        self.talk(b'[1,2,3]\n')
        self.assertTrue(self.server.acquire_lock(self.lock_path('after'), timeout=1)[0])


class TestHandshakeTimeout(LockerTestCase):
    """Checks that a connection that never says hello is dropped."""

    def setUp(self):
        """Creates a server with a logger and a short handshake timeout."""
        super().setUp()
        patcher = mock.patch.object(lockerModule, '_HANDSHAKE_TIMEOUT', 1.0)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.logger = logging.getLogger('pylocker.test.handshake')
        self.server = self.make_locker(self.server_file(), logger=self.logger)
        self.address, self.port = self.server_address(self.server)

    def read_until_closed(self, connection, wait=4.0):
        """Waits for the server to close a connection.

        :Parameters:
            #. connection (socket.socket): The open connection.
            #. wait (int, float): The longest wait in seconds.

        :Returns:
            #. isClosed (bool): Whether the server closed the connection.
            #. seconds (float): How long it took.
        """
        connection.settimeout(wait)
        start = time.time()
        try:
            isClosed = connection.recv(10) == b''
        except socket.timeout:
            isClosed = False
        return isClosed, time.time() - start

    def test_silent_connection_is_dropped(self):
        """Closes a connection that sends nothing, after the timeout."""
        with socket.create_connection((self.address, self.port)) as connection:
            isClosed, seconds = self.read_until_closed(connection)
        self.assertTrue(isClosed)
        self.assertTrue(0.8 < seconds < 2.5, seconds)

    def test_half_a_hello_is_dropped(self):
        """Closes a connection whose hello never ends."""
        with socket.create_connection((self.address, self.port)) as connection:
            connection.sendall(b'{"action": "hello", "name": "x"')
            isClosed, seconds = self.read_until_closed(connection)
        self.assertTrue(isClosed)
        self.assertTrue(0.8 < seconds < 2.5, seconds)

    def test_warning_is_logged(self):
        """Logs a warning for the dropped connection."""
        with self.assertLogs(self.logger, level='WARNING') as captured:
            with socket.create_connection((self.address, self.port)) as connection:
                self.read_until_closed(connection)
        self.assertTrue(any('no hello message' in line for line in captured.output))

    def test_stalled_connection_does_not_block_others(self):
        """Lets a real client connect while another connection is stalled."""
        with socket.create_connection((self.address, self.port)):
            start = time.time()
            client = self.make_client(self.server.serverFile)
            self.assertTrue(client.isClient)
            self.assertLess(time.time() - start, 1.0)
            self.assertTrue(client.acquire_lock(self.lock_path('h1'), timeout=2)[0])

    def test_many_stalled_connections_are_all_dropped(self):
        """Closes fifty stalled connections."""
        connections = [socket.create_connection((self.address, self.port)) for _ in range(50)]
        try:
            time.sleep(1.8)
            alive = 0
            for connection in connections:
                connection.settimeout(0.2)
                try:
                    if connection.recv(10) != b'':
                        alive += 1
                except socket.timeout:
                    alive += 1
                except OSError:
                    pass
        finally:
            for connection in connections:
                connection.close()
        self.assertEqual(alive, 0)
        self.assertTrue(self.server.acquire_lock(self.lock_path('h2'), timeout=1)[0])


class TestLocalOnlyServing(LockerTestCase):
    """Checks that a server accepts only local connections unless it is told otherwise."""

    def test_server_advertises_the_loopback_address(self):
        """Writes 127.0.0.1 as the server address by default."""
        server = self.make_locker(self.server_file())
        self.assertEqual(self.server_address(server)[0], '127.0.0.1')

    def test_remote_serving_is_opt_in(self):
        """Reports allowRemoteClients as False until it is switched on."""
        self.assertFalse(self.make_locker(self.server_file('a.lock')).allowRemoteClients)
        self.assertTrue(self.make_locker(self.server_file('b.lock'), allowRemoteClients=True).allowRemoteClients)

    def test_default_server_refuses_the_machine_address(self):
        """Refuses a connection made to the machine's network address when remote clients are not allowed."""
        hostAddress = lockerModule._get_ip()
        if hostAddress == '127.0.0.1':
            self.skipTest('this machine has no network address besides loopback')
        server = self.make_locker(self.server_file())
        _, port = self.server_address(server)
        with self.assertRaises(OSError):
            socket.create_connection((hostAddress, port), timeout=2)

    def test_remote_server_accepts_the_machine_address(self):
        """Accepts connections to the machine's network address, and lets a client lock through it."""
        server = self.make_locker(self.server_file(), allowRemoteClients=True)
        address, port = self.server_address(server)
        if address == '127.0.0.1':
            self.skipTest('this machine has no network address besides loopback')
        with socket.create_connection((address, port), timeout=2):
            pass
        client = self.make_client(self.server_file(), allowRemoteClients=True)
        self.assertTrue(client.isClient)
        self.assertTrue(client.acquire_lock(self.lock_path('remote'), timeout=2)[0])

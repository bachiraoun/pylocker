"""Tests that a client that stops reading, or floods the queue with large requests, cannot hold up the others."""
import json
import socket
import time
import unittest
import uuid
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, PASSWORD, login_by_hand, wait_until


class TestPeerThatStopsReading(LockerTestCase):
    """Checks that a connection whose peer never reads is closed instead of holding up every lock grant."""

    def open_connection(self, port, uniqueName, receiveBuffer=None):
        """Logs in by hand over a connection that is closed when the test ends.

        :Parameters:
            #. port (int): The port of the server.
            #. uniqueName (str): The unique name in the hello message.
            #. receiveBuffer (int, None): Size of the receive buffer, small to make it fill quickly.

        :Returns:
            #. connection (socket.socket): The logged in connection.
        """
        connection = socket.socket()
        if receiveBuffer is not None:
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receiveBuffer)
        connection.connect(('127.0.0.1', port))
        connection.settimeout(0.3)
        reader = connection.makefile('rb')
        self.addCleanup(connection.close)
        self.addCleanup(reader.close)
        self.assertEqual(login_by_hand(connection, reader, uniqueName=uniqueName)['action'], 'welcome')
        return connection

    def shorten_write_timeout(self):
        """Lets the server wait one second, instead of ten, for a peer that does not read, until the test ends."""
        patcher = mock.patch.object(lockerModule, '_WRITE_TIMEOUT_SECONDS', 1.0)
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def send(connection, document):
        """Sends one JSON message.

        :Parameters:
            #. connection (socket.socket): The connection.
            #. document (dict): The message.
        """
        connection.sendall(json.dumps(document).encode('utf-8') + b'\n')

    def fill_connection_of(self, publisher):
        """Publishes large messages until the server cannot send more to the connections that do not read.

        :Parameters:
            #. publisher (socket.socket): The connection that publishes.
        """
        text = 'A' * 60000
        try:
            for index in range(400):
                self.send(publisher, {'action': 'publish', 'message': text + str(index), 'replace': False,
                                      'receivers': None, 'toSelf': False, 'timeout': None,
                                      'publication_id': 'x'})
        except socket.timeout:
            # The sending side is blocked too, which shows that the buffers between the two are full
            pass

    def test_lock_grants_continue_after_the_silent_peer_is_closed(self):
        """Closes the connection of a peer that does not read, and keeps granting locks to the others."""
        self.shorten_write_timeout()
        server, holder = self.make_pair()
        healthy = self.make_client(server.serverFile)
        acquired, lockId = holder.acquire_lock('shared')
        self.assertTrue(acquired)
        silent = self.open_connection(server._serverPort, 'silent', receiveBuffer=4096)
        self.send(silent, {'action': 'acquire', 'request_unique_id': str(uuid.uuid4()), 'path': ['shared'],
                           'timeout': 60, 'request_utctime': time.time()})
        publisher = self.open_connection(server._serverPort, 'publisher')
        self.fill_connection_of(publisher)
        holder.release_lock(lockId)
        started = time.time()
        acquired, _ = healthy.acquire_lock('other', timeout=8, retries=0)
        self.assertTrue(acquired)
        self.assertLess(time.time() - started, 6)

    def test_silent_peer_is_logged_and_dropped(self):
        """Warns about the connection it closes, and forgets the client."""
        messages = []
        self.shorten_write_timeout()
        server = self.make_locker(self.server_file())
        server._warn = messages.append
        silent = self.open_connection(server._serverPort, 'silent', receiveBuffer=4096)
        self.send(silent, {'action': 'acquire', 'request_unique_id': str(uuid.uuid4()), 'path': ['shared'],
                           'timeout': 60, 'request_utctime': time.time()})
        publisher = self.open_connection(server._serverPort, 'publisher')
        self.fill_connection_of(publisher)
        self.assertTrue(wait_until(lambda: any('did not read' in message for message in messages), seconds=8))
        self.assertTrue(wait_until(lambda: 'silent' not in server._clientsLUT, seconds=8))


class TestLargeRequestsInTheQueue(LockerTestCase):
    """Checks that waiting requests for many paths stay cheap to check."""

    def test_request_cannot_choose_its_own_path_set(self):
        """Ignores a path set sent by a client, so a held path always keeps the request waiting."""
        server, holder = self.make_pair()
        acquired, _ = holder.acquire_lock('held')
        self.assertTrue(acquired)
        raw = socket.create_connection(('127.0.0.1', server._serverPort), timeout=1)
        reader = raw.makefile('rb')
        self.addCleanup(raw.close)
        self.addCleanup(reader.close)
        self.assertEqual(login_by_hand(raw, reader, uniqueName='raw')['action'], 'welcome')
        raw.sendall(json.dumps({'action': 'acquire', 'request_unique_id': str(uuid.uuid4()), 'path': ['held'],
                                'timeout': 60, 'request_utctime': time.time(),
                                '_pathSet': []}).encode('utf-8') + b'\n')
        with self.assertRaises(socket.timeout):
            reader.readline()

    def test_many_waiting_requests_for_many_paths_do_not_slow_the_others(self):
        """Keeps a lock request fast while a client has 1000 requests of 1000 paths waiting."""
        server, holder = self.make_pair()
        other = self.make_client(server.serverFile)
        acquired, _ = holder.acquire_lock('held')
        self.assertTrue(acquired)
        raw = socket.create_connection(('127.0.0.1', server._serverPort), timeout=5)
        reader = raw.makefile('rb')
        self.addCleanup(raw.close)
        self.addCleanup(reader.close)
        self.assertEqual(login_by_hand(raw, reader, uniqueName='raw')['action'], 'welcome')
        for index in range(1000):
            paths = [f'p{index}_{number}' for number in range(999)] + ['held']
            raw.sendall(json.dumps({'action': 'acquire', 'request_unique_id': str(uuid.uuid4()), 'path': paths,
                                    'timeout': 120, 'request_utctime': time.time()}).encode('utf-8') + b'\n')
        time.sleep(2)
        delays = []
        for index in range(5):
            started = time.time()
            acquired, lockId = other.acquire_lock(f'free{index}', timeout=5, retries=0)
            delays.append(time.time() - started)
            self.assertTrue(acquired)
            other.release_lock(lockId)
        self.assertLess(min(delays), 0.04)


if __name__ == '__main__':
    unittest.main()

"""Tests how a client reacts to unusual messages, using a small fake server that sends exactly what a test tells it to."""
import json
import logging
import socket
import threading
import unittest
from unittest import mock

from pylocker import Locker as lockerModule
from pylocker import ServerLocker

from helpers import LockerTestCase, PASSWORD, wait_until


class FakeServer:
    """
    A server that answers the handshake, grants every lock request, and sends whatever a test asks.

    :Parameters:
        #. serializer (str): The serializer announced in the welcome message.
        #. autoGrant (bool): Whether every acquire request is granted at once.
    """

    def __init__(self, serializer='json', autoGrant=True):
        """Starts listening on a free local port."""
        self.serializer = serializer
        self.autoGrant = autoGrant
        self.received = []
        self.connection = None
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        """Accepts one client, answers its hello and records what it sends."""
        try:
            connection, _ = self.listener.accept()
        except OSError:
            return
        self.connection = connection
        reader = connection.makefile('rb')
        reader.readline()
        self.send({'action': 'welcome', 'server_name': 'fake', 'server_unique_name': 'fake-id',
                   'server_address': '127.0.0.1', 'server_port': self.port, 'lock_maximum_acquired_time': 60,
                   'server_file': '', 'password_ok': True, 'serializer': self.serializer})
        try:
            for line in reader:
                message = json.loads(line)
                self.received.append(message)
                if self.autoGrant and message.get('action') == 'acquire':
                    self.send({'action': 'acquired', 'request_unique_id': message['request_unique_id']})
        except (OSError, ValueError):
            pass

    def send(self, document):
        """Sends one JSON message to the client.

        :Parameters:
            #. document (dict): The message.
        """
        self.send_raw(json.dumps(document).encode('utf-8') + b'\n')

    def send_raw(self, data):
        """Sends bytes to the client as they are.

        :Parameters:
            #. data (bytes): The bytes to send.
        """
        self.connection.sendall(data)

    def drop_client(self):
        """Closes the connection to the client; shutdown is needed because the reader still holds the socket."""
        self.connection.shutdown(socket.SHUT_RDWR)
        self.connection.close()

    def close(self):
        """Closes the connection and stops listening."""
        for closable in (self.connection, self.listener):
            try:
                closable.shutdown(socket.SHUT_RDWR)
            except (OSError, AttributeError):
                pass
            try:
                closable.close()
            except (OSError, AttributeError):
                pass


class TestClientAgainstAFakeServer(LockerTestCase):
    """Checks that a client copes with whatever a server sends."""

    def connect_client(self, fakeServer, **kwargs):
        """Connects a client to the fake server.

        :Parameters:
            #. fakeServer (FakeServer): The server to connect to.
            #. kwargs (dict): Arguments of ``ServerLocker``, replacing the defaults.

        :Returns:
            #. client (ServerLocker): The client, connected unless the connection failed.
        """
        arguments = {'password': PASSWORD, 'serverFile': False, 'autoconnect': False, 'allowServing': False,
                     'connectTimeout': 3}
        arguments.update(kwargs)
        client = self.track(ServerLocker(**arguments))
        client.start(address='127.0.0.1', port=fakeServer.port, ntrials=1)
        return client

    def setUp(self):
        """Starts a fake server and connects a client to it."""
        super().setUp()
        self.fake = FakeServer()
        self.addCleanup(self.fake.close)
        self.client = self.connect_client(self.fake)
        self.assertTrue(self.client.isClient)

    def test_garbage_line_is_skipped(self):
        """Keeps reading after a line that is no valid message."""
        self.fake.send_raw(b'this is not json\n')
        self.assertTrue(self.client.acquire_lock(self.lock_path('a'), timeout=2)[0])
        self.assertTrue(self.client.isClient)

    def test_unknown_action_is_ignored(self):
        """Ignores a message with an action it does not know."""
        self.fake.send({'action': 'mystery'})
        self.assertTrue(self.client.acquire_lock(self.lock_path('b'), timeout=2)[0])

    def test_eviction_notice_drops_the_lock(self):
        """Removes a lock from the holder's list when the server says it was evicted."""
        acquired, lockId = self.client.acquire_lock(self.lock_path('c'), timeout=2)
        self.assertTrue(acquired)
        self.assertIn(lockId, self.client.ownedLocks)
        self.fake.send({'action': 'exceeded_maximum_lock_time', 'request_unique_id': lockId})
        self.assertTrue(wait_until(lambda: lockId not in self.client.ownedLocks))

    def test_late_grant_is_given_back(self):
        """Sends a release for a lock that was granted after the caller gave up."""
        self.fake.send({'action': 'acquired', 'request_unique_id': 'late-grant'})
        isReleased = wait_until(lambda: any(message.get('action') == 'release' and
                                            message.get('request_unique_id') == 'late-grant'
                                            for message in self.fake.received))
        self.assertTrue(isReleased)
        self.assertNotIn('late-grant', self.client.ownedLocks)

    def test_publication_from_the_server_is_stored(self):
        """Stores a message the server publishes."""
        self.fake.send({'action': 'publish', 'message': 'news', 'publisher_unique_name': 'fake-id', 'replace': True})
        self.assertTrue(wait_until(lambda: self.client.has_message('news')))

    def test_order_with_the_wrong_password_is_ignored(self):
        """Stays connected when the server sends a stop order with the wrong password."""
        self.fake.send({'action': 'order', 'order': 'stop', 'args': [False], 'password': 'wrong',
                        'client_unique_name': 'fake-id'})
        self.assertFalse(wait_until(lambda: not self.client.isClient, seconds=1))

    def test_order_with_the_right_password_stops_the_client(self):
        """Stops the client when the server sends a stop order with the right password."""
        self.fake.send({'action': 'order', 'order': 'stop', 'args': [False], 'password': PASSWORD,
                        'client_unique_name': 'fake-id'})
        self.assertTrue(wait_until(lambda: not self.client.isClient))

    def test_order_with_an_unknown_name_is_ignored(self):
        """Stays connected when the order is not a stop order."""
        self.fake.send({'action': 'order', 'order': 'dance', 'args': [], 'password': PASSWORD,
                        'client_unique_name': 'fake-id'})
        self.assertFalse(wait_until(lambda: not self.client.isClient, seconds=1))

    def test_dropped_connection_ends_the_client(self):
        """Ends the client when the server closes the connection."""
        self.fake.drop_client()
        self.assertTrue(wait_until(lambda: not self.client.isClient))
        self.assertEqual(self.client.acquire_lock(self.lock_path('d'), timeout=1), (False, 2))


class TestSilentServer(LockerTestCase):
    """Checks a server that never answers lock requests."""

    def test_waiting_call_fails_when_the_connection_drops(self):
        """Returns a connection-lost error to a caller that waits for a grant that never comes."""
        fake = FakeServer(autoGrant=False)
        self.addCleanup(fake.close)
        client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                         connectTimeout=3))
        client.start(address='127.0.0.1', port=fake.port, ntrials=1)
        result = {}

        def wait():
            """Waits for a grant and keeps the answer."""
            result['answer'] = client.acquire_lock(self.lock_path('never'), timeout=30, retries=0)

        thread = threading.Thread(target=wait)
        thread.start()
        self.assertTrue(wait_until(lambda: len(fake.received) > 0))
        fake.drop_client()
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result['answer'], (False, 'Server connection lost'))


class TestServerThatNeedsMsgpack(LockerTestCase):
    """Checks a client that cannot follow the serializer its server announces."""

    def test_client_without_msgpack_does_not_connect(self):
        """Stays unconnected and logs an error when the server announces msgpack and the package is missing."""
        fake = FakeServer(serializer='msgpack')
        self.addCleanup(fake.close)
        logger = logging.getLogger('pylocker.test.needsmsgpack')
        client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                         connectTimeout=3, logger=logger))
        with mock.patch.object(lockerModule, 'MSGPACK_AVAILABLE', False):
            with self.assertLogs(logger, level='ERROR') as captured:
                client.start(address='127.0.0.1', port=fake.port, ntrials=1)
        self.assertFalse(client.isClient)
        self.assertTrue(any('requires msgpack' in line for line in captured.output))


if __name__ == '__main__':
    unittest.main()

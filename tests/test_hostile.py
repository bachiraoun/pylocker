"""Tests that a client sending broken requests cannot stop the server from serving the other clients."""
import json
import socket

from pylocker import ServerLocker

from helpers import LockerTestCase, login_by_hand, wait_until


class RawClient:
    """
    A connection that speaks the wire protocol by hand, so a test can send requests no real client sends.

    :Parameters:
        #. port (int): The port of the server.
        #. uniqueName (str): The unique name in the hello message.
        #. loginArguments (dict): More arguments for ``login_by_hand``.
    """

    def __init__(self, port, uniqueName='raw-client', **loginArguments):
        """Connects and completes the handshake; the last message of the server is kept in ``reply``."""
        self.connection = socket.create_connection(('127.0.0.1', port), timeout=5)
        self.reader = self.connection.makefile('rb')
        self.reply = login_by_hand(self.connection, self.reader, uniqueName=uniqueName, **loginArguments)

    def send(self, document):
        """Sends one JSON message.

        :Parameters:
            #. document (dict): The message.
        """
        self.connection.sendall(json.dumps(document).encode('utf-8') + b'\n')

    def read(self):
        """Reads one JSON message.

        :Returns:
            #. document (dict): The message.
        """
        return json.loads(self.reader.readline())

    def close(self):
        """Closes the connection."""
        self.reader.close()
        self.connection.close()


def acquire_request(**changes):
    """Builds a valid acquire request and applies changes to it.

    :Parameters:
        #. changes (dict): Fields to set; a value of the string ``<remove>`` removes the field.

    :Returns:
        #. request (dict): The request.
    """
    request = {'action': 'acquire', 'request_unique_id': 'raw-1', 'path': ['raw/path'], 'timeout': 5,
               'client_unique_name': 'raw-client'}
    for key, value in changes.items():
        if value == '<remove>':
            request.pop(key, None)
        else:
            request[key] = value
    return request


BAD_REQUESTS = {
    'no id': acquire_request(request_unique_id='<remove>'),
    'id not text': acquire_request(request_unique_id=5),
    'no path': acquire_request(path='<remove>'),
    'path is a number': acquire_request(path=5),
    'path is empty': acquire_request(path=[]),
    'path has a number': acquire_request(path=['ok', 7]),
    'path has a list': acquire_request(path=[['x']]),
    'timeout is text': acquire_request(timeout='soon'),
    'timeout is boolean': acquire_request(timeout=True),
    'timeout is negative': acquire_request(timeout=-1),
    'timeout is zero': acquire_request(timeout=0),
    'time is text': acquire_request(request_utctime='yesterday'),
    'client name is a list': acquire_request(client_unique_name=['x']),
}


class TestMalformedRequests(LockerTestCase):
    """Checks that broken acquire requests are dropped and everyone else is still served."""

    def setUp(self):
        """Starts a server, a normal client and a hand-made client."""
        super().setUp()
        self.server, self.client = self.make_pair()
        self.raw = RawClient(self.server._serverPort)
        self.addCleanup(self.raw.close)

    def assert_server_still_serves(self):
        """Fails unless a normal client can lock and every background task of the server is running."""
        acquired, lockId = self.client.acquire_lock(self.lock_path('after'), timeout=3, retries=0)
        self.assertTrue(acquired)
        self.client.release_lock(lockId)
        self.assertFalse(any(task.done() for task in self.server._bgTasks))

    def test_every_broken_request_is_dropped(self):
        """Keeps serving after each kind of broken acquire request."""
        for label, request in BAD_REQUESTS.items():
            with self.subTest(label):
                self.raw.send(request)
                self.assert_server_still_serves()

    def test_valid_request_is_granted(self):
        """Grants a correct acquire request sent by hand."""
        self.raw.send(acquire_request())
        reply = self.raw.read()
        self.assertEqual((reply['action'], reply['request_unique_id']), ('acquired', 'raw-1'))

    def test_request_without_optional_fields_is_granted(self):
        """Grants a request that has no timeout, time or client name."""
        self.raw.send(acquire_request(timeout='<remove>', client_unique_name='<remove>'))
        self.assertEqual(self.raw.read()['action'], 'acquired')

    def test_future_sent_by_a_client_is_ignored(self):
        """Grants the request normally when a client adds a field that only the server may set."""
        self.raw.send(acquire_request(_future=1))
        self.assertEqual(self.raw.read()['action'], 'acquired')
        self.assert_server_still_serves()

    def test_queue_survives_a_broken_request_that_bypassed_the_check(self):
        """Drops one broken queued request, logs it, and goes on granting the others."""
        broken = {'action': 'acquire', 'request_unique_id': 'broken', 'path': 5}
        self.server._loop.call_soon_threadsafe(self.server._lockQueue.put_nowait, broken)
        self.server._loop.call_soon_threadsafe(self.server._queueEvent.set)
        self.assert_server_still_serves()


class TestAcquireRequestProblem(LockerTestCase):
    """Checks the rules that decide whether an acquire request is accepted."""

    def test_valid_requests_have_no_problem(self):
        """Returns None for correct requests, with or without the optional fields."""
        for request in (acquire_request(), acquire_request(timeout=2.5, request_utctime=1.0e9),
                        acquire_request(timeout='<remove>', client_unique_name='<remove>')):
            self.assertIsNone(ServerLocker._acquire_request_problem(request))

    def test_bad_numbers_are_named(self):
        """Names the field when a number is not finite."""
        for value in (float('nan'), float('inf')):
            problem = ServerLocker._acquire_request_problem(acquire_request(timeout=value))
            self.assertEqual(problem, 'timeout must be a finite number')
            problem = ServerLocker._acquire_request_problem(acquire_request(request_utctime=value))
            self.assertEqual(problem, 'request_utctime must be a finite number')

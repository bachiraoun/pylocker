"""Tests the limits on messages, connections and waiting requests, ownership of locks and names, and safe logging."""
import asyncio
import logging
import pickle
import socket
import struct
import time
import unittest

from pylocker import AuthenticationError, ServerLocker
from pylocker import Locker as lockerModule

from helpers import LockerTestCase, PASSWORD, wait_until
from test_hostile import RawClient, acquire_request

try:
    import msgpack
except ImportError:
    msgpack = None


class TestMessageSize(LockerTestCase):
    """Checks that a message above the limit ends the connection and is never read."""

    def setUp(self):
        """Creates a server whose limit is 2048 bytes, and a normal client."""
        super().setUp()
        self.server = self.make_locker(self.server_file(), maxMessageBytes=2048)
        self.client = self.make_client(self.server.serverFile)
        self.port = self.server._serverPort

    def test_long_json_line_ends_the_connection(self):
        """Closes the connection of a client that sends a line above the limit, and keeps serving the others."""
        raw = RawClient(self.port)
        self.addCleanup(raw.close)
        raw.connection.sendall(b'{"action":"publish","message":"' + b'a' * 5000 + b'"}\n')
        self.assertTrue(wait_until(lambda: 'raw-client' not in self.server._clientsLUT))
        self.assertTrue(self.client.acquire_lock(self.lock_path('after'), timeout=2)[0])

    def test_line_below_the_limit_is_served(self):
        """Serves a request that fits in the limit."""
        raw = RawClient(self.port)
        self.addCleanup(raw.close)
        raw.send(acquire_request(path=['p' * 800]))
        self.assertEqual(raw.read()['action'], 'acquired')

    @unittest.skipIf(msgpack is None, 'needs the msgpack package')
    def test_huge_msgpack_frame_ends_the_connection(self):
        """Closes the connection of a client that announces a frame of four gigabytes, without waiting for it."""
        server = self.make_locker(self.server_file('mp.lock'), serializer='msgpack', maxMessageBytes=2048)
        raw = RawClient(server._serverPort)
        self.addCleanup(raw.close)
        raw.connection.sendall(struct.pack('>I', 4_000_000_000))
        self.assertTrue(wait_until(lambda: 'raw-client' not in server._clientsLUT, seconds=3))
        self.assertTrue(server.acquire_lock(self.lock_path('mp'), timeout=2)[0])

    def test_frame_size_is_checked_before_the_frame_is_read(self):
        """Raises at once when only the header of an oversized frame has arrived."""
        async def read():
            reader = asyncio.StreamReader()
            reader.feed_data(struct.pack('>I', 4_000_000_000))
            return await lockerModule._read_msgpack(reader, 2048)

        with self.assertRaises(lockerModule._MessageTooLargeError):
            asyncio.run(read())

    def test_client_refuses_to_publish_a_message_above_the_limit(self):
        """Raises ValueError for a message that is too long, and delivers one that fits."""
        client = self.make_client(self.server.serverFile, maxMessageBytes=2048)
        with self.assertRaisesRegex(ValueError, 'longer than'):
            client.publish_message('x' * 3000)
        self.assertTrue(client.publish_message('x' * 500)[0])

    def test_bad_settings_are_refused(self):
        """Raises ValueError for limits that are not whole numbers, are too small, or are below the minimum."""
        for kwargs in ({'maxMessageBytes': 100}, {'maxMessageBytes': 'x'}, {'maxMessageBytes': True},
                       {'maxClients': 0}, {'maxClients': 1.5}, {'maxPendingPerClient': -1}):
            with self.subTest(kwargs):
                with self.assertRaises(ValueError):
                    ServerLocker(PASSWORD, serverFile=False, autoconnect=False, **kwargs)


class TestConnectionLimit(LockerTestCase):
    """Checks that the server holds no more clients than it is told to."""

    def test_client_over_the_limit_is_refused_and_a_free_slot_is_reused(self):
        """Refuses a third client after two have logged in, until one of them is gone."""
        server = self.make_locker(self.server_file(), maxClients=2)
        port = server._serverPort
        first = RawClient(port, uniqueName='first')
        second = RawClient(port, uniqueName='second')
        for client in (first, second):
            self.addCleanup(client.close)
            self.assertEqual(client.reply['action'], 'welcome')
        refused = RawClient(port, uniqueName='third')
        self.addCleanup(refused.close)
        self.assertEqual(refused.reply, {'action': 'error', 'reason': 'server full'})
        first.close()
        self.assertTrue(wait_until(lambda: 'first' not in server._clientsLUT))
        accepted = RawClient(port, uniqueName='fourth')
        self.addCleanup(accepted.close)
        self.assertEqual(accepted.reply['action'], 'welcome')

    def test_real_client_is_not_told_it_has_a_wrong_password(self):
        """Raises no AuthenticationError for a full server, and does not connect."""
        server = self.make_locker(self.server_file(), maxClients=1)
        holder = RawClient(server._serverPort, uniqueName='holder')
        self.addCleanup(holder.close)
        client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                         connectTimeout=3))
        client.start(address='127.0.0.1', port=server._serverPort, connectAttempts=1)
        self.assertFalse(client.isClient)


class TestWaitingRequests(LockerTestCase):
    """Checks the limit on lock requests that wait, and the limit on paths in one request."""

    def setUp(self):
        """Creates a server that lets a client wait with three requests, and holds one path."""
        super().setUp()
        self.server = self.make_locker(self.server_file(), maxPendingPerClient=3)
        self.path = self.lock_path('busy')
        self.assertTrue(self.server.acquire_lock(self.path, timeout=2)[0])
        self.raw = RawClient(self.server._serverPort)
        self.addCleanup(self.raw.close)

    def waiting(self):
        """Counts the requests the server holds for the hand-made client.

        :Returns:
            #. count (int): The number of waiting requests.
        """
        return sum(self.server._pendingCounts.values())

    def test_extra_requests_are_dropped(self):
        """Keeps at most the allowed number of requests waiting."""
        for index in range(6):
            self.raw.send(acquire_request(request_unique_id=f'r{index}', path=[self.path], timeout=3))
        self.assertTrue(wait_until(lambda: self.waiting() == 3))
        time.sleep(0.5)
        self.assertEqual(self.waiting(), 3)

    def test_slots_are_freed_when_requests_expire(self):
        """Accepts new requests after the waiting ones have expired."""
        for index in range(3):
            self.raw.send(acquire_request(request_unique_id=f'r{index}', path=[self.path], timeout=1))
        self.assertTrue(wait_until(lambda: self.waiting() == 3))
        self.assertTrue(wait_until(lambda: self.waiting() == 0, seconds=5))
        self.raw.send(acquire_request(request_unique_id='later', path=[self.lock_path('free')]))
        self.assertEqual(self.raw.read()['request_unique_id'], 'later')

    def test_request_without_a_time_still_expires(self):
        """Drops a waiting request that carries no request time once its timeout has passed."""
        request = acquire_request(request_unique_id='no-time', path=[self.path], timeout=1)
        self.assertNotIn('request_utctime', request)
        self.raw.send(request)
        self.assertTrue(wait_until(lambda: self.waiting() == 1))
        self.assertTrue(wait_until(lambda: self.waiting() == 0, seconds=5))

    def test_counts_are_cleared_when_the_client_leaves(self):
        """Forgets the waiting requests of a client that disconnected."""
        for index in range(2):
            self.raw.send(acquire_request(request_unique_id=f'r{index}', path=[self.path], timeout=20))
        self.assertTrue(wait_until(lambda: self.waiting() == 2))
        self.raw.close()
        self.assertTrue(wait_until(lambda: self.server._pendingCounts == {}))

    def test_other_clients_are_not_affected(self):
        """Serves a normal client while another one is at its limit."""
        for index in range(5):
            self.raw.send(acquire_request(request_unique_id=f'r{index}', path=[self.path], timeout=3))
        client = self.make_client(self.server.serverFile)
        self.assertTrue(client.acquire_lock(self.lock_path('other'), timeout=2)[0])

    def test_request_with_too_many_paths_is_dropped(self):
        """Drops a request with 1001 paths and grants one with 1000."""
        self.raw.send(acquire_request(request_unique_id='big', path=[f'many/{index}' for index in range(1001)]))
        self.raw.send(acquire_request(request_unique_id='ok', path=[f'many/{index}' for index in range(1000)]))
        self.assertEqual(self.raw.read()['request_unique_id'], 'ok')

    def test_client_refuses_to_ask_for_too_many_paths(self):
        """Raises ValueError for a call with more paths than a request may hold."""
        client = self.make_client(self.server.serverFile)
        with self.assertRaisesRegex(ValueError, 'more than 1000'):
            client.acquire_lock([f'many/{index}' for index in range(1001)])


class TestOwnerOnlyRelease(LockerTestCase):
    """Checks that only the connection that holds a lock can release it."""

    def setUp(self):
        """Creates a server, a real client and a hand-made client."""
        super().setUp()
        self.server, self.client = self.make_pair()
        self.raw = RawClient(self.server._serverPort, uniqueName='raw-thief')
        self.addCleanup(self.raw.close)

    def is_free(self, path):
        """Tells whether the server can lock a path at once.

        :Parameters:
            #. path (str): The path to try.

        :Returns:
            #. isFree (bool): Whether the lock was granted.
        """
        acquired, lockId = self.server.acquire_lock(path, timeout=0.3, retries=0)
        if acquired:
            self.server.release_lock(lockId)
        return acquired

    def test_other_connection_cannot_release_a_lock(self):
        """Keeps a lock held when another connection sends its release."""
        path = self.lock_path('mine')
        acquired, lockId = self.client.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.raw.send({'action': 'release', 'request_unique_id': lockId})
        time.sleep(0.5)
        self.assertFalse(self.is_free(path))
        self.client.release_lock(lockId)
        self.assertTrue(self.is_free(path))

    def test_client_cannot_release_a_lock_of_the_server(self):
        """Keeps a lock of the server held when a client sends its release."""
        path = self.lock_path('server-lock')
        acquired, lockId = self.server.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.raw.send({'action': 'release', 'request_unique_id': lockId})
        time.sleep(0.5)
        self.assertFalse(self.is_free(path))
        self.server.release_lock(lockId)

    def test_connection_releases_its_own_lock(self):
        """Releases a lock when the connection that holds it asks."""
        path = self.lock_path('own')
        self.raw.send(acquire_request(request_unique_id='own-1', path=[path]))
        self.assertEqual(self.raw.read()['action'], 'acquired')
        self.raw.send({'action': 'release', 'request_unique_id': 'own-1'})
        self.assertTrue(wait_until(lambda: self.is_free(path)))


class TestUniqueNames(LockerTestCase):
    """Checks that two connections cannot use the same unique name."""

    def setUp(self):
        """Creates a server and a real client."""
        super().setUp()
        self.server, self.client = self.make_pair()

    def test_second_connection_with_a_name_in_use_is_refused(self):
        """Refuses a connection that takes the name of a connected client, and leaves that client alone."""
        path = self.lock_path('keep')
        acquired, lockId = self.client.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        duplicate = RawClient(self.server._serverPort, uniqueName=self.client.uniqueName)
        self.addCleanup(duplicate.close)
        self.assertEqual(duplicate.reply, {'action': 'error', 'reason': 'name in use'})
        time.sleep(0.5)
        self.assertIn(self.client.uniqueName, self.server._clientsLUT)
        self.assertEqual(self.server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        self.client.release_lock(lockId)

    def test_name_of_the_server_is_refused(self):
        """Refuses a connection that takes the unique name of the server."""
        duplicate = RawClient(self.server._serverPort, uniqueName=self.server.uniqueName)
        self.addCleanup(duplicate.close)
        self.assertEqual(duplicate.reply['reason'], 'name in use')

    def test_name_is_free_again_after_the_client_leaves(self):
        """Accepts the name again once its client has gone."""
        name = self.client.uniqueName
        self.client.stop()
        self.assertTrue(wait_until(lambda: name not in self.server._clientsLUT))
        again = RawClient(self.server._serverPort, uniqueName=name)
        self.addCleanup(again.close)
        self.assertEqual(again.reply['action'], 'welcome')

    def test_invalid_names_are_refused(self):
        """Refuses names that are not text or are too long, and keeps serving."""
        for label, arguments in (('list', {'uniqueName': ['x']}), ('too long', {'uniqueName': 'n' * 300}),
                                 ('name number', {'name': 5}), ('name too long', {'name': 'n' * 300})):
            with self.subTest(label):
                raw = RawClient(self.server._serverPort, **arguments)
                self.addCleanup(raw.close)
                self.assertEqual(raw.reply, {'action': 'error', 'reason': 'invalid hello'})
        self.assertTrue(self.client.acquire_lock(self.lock_path('still'), timeout=2)[0])

    def test_copy_has_an_identity_of_its_own(self):
        """Gives a copy a new unique name, so it joins the server next to the original."""
        clone = pickle.loads(pickle.dumps(self.client))
        self.assertNotEqual(clone.uniqueName, self.client.uniqueName)
        clone.set_password(PASSWORD)
        self.track(clone)
        clone.start()
        self.assertTrue(clone.isClient and self.client.isClient)


class TestBinaryPassword(LockerTestCase):
    """Checks passwords that are not valid text."""

    def test_passwords_that_are_not_text_are_told_apart(self):
        """Connects with the same bytes and refuses bytes that differ, even though both decode to the same text."""
        first, second = b'\xff\xfe\xfd', b'\xff\xfe\xfc'
        self.assertEqual(first.decode('utf-8', errors='replace'), second.decode('utf-8', errors='replace'))
        serverFile = self.server_file()
        self.make_locker(serverFile, password=first)
        self.assertTrue(self.make_client(serverFile, password=first).isClient)
        with self.assertRaises(AuthenticationError):
            ServerLocker(second, serverFile=serverFile, connectTimeout=3, allowServing=False)


class TestLogSafety(LockerTestCase):
    """Checks that text from other programs cannot forge log lines."""

    def test_names_with_line_breaks_stay_on_one_line(self):
        """Writes a client name and an action that hold line breaks as escaped text."""
        logger = logging.getLogger('pylocker.test.logsafety')
        server = self.make_locker(self.server_file(), logger=logger, debugMode=True)
        with self.assertLogs(logger, level='DEBUG') as captured:
            raw = RawClient(server._serverPort, uniqueName='evil\nCRITICAL forged line')
            self.addCleanup(raw.close)
            raw.send({'action': 'x\nCRITICAL forged action'})
            time.sleep(0.5)
        messages = [record.getMessage() for record in captured.records]
        self.assertTrue(any('forged line' in message for message in messages))
        self.assertTrue(any('forged action' in message for message in messages))
        self.assertFalse(any('\n' in message for message in messages), messages)

    def test_log_text_shortens_and_quotes(self):
        """Quotes the text, escapes line breaks and shortens long text."""
        self.assertEqual(lockerModule._log_text('a\nb'), "'a\\nb'")
        self.assertTrue(lockerModule._log_text('x' * 500).endswith("'..."))
        self.assertLess(len(lockerModule._log_text('x' * 500)), 120)
        self.assertEqual(lockerModule._log_text(None), "'None'")


if __name__ == '__main__':
    unittest.main()

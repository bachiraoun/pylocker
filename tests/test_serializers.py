"""Tests the JSON and msgpack serializers and how a client follows the choice of its server."""
import logging
import unittest
from unittest import mock

from pylocker import Locker as lockerModule
from pylocker import AuthenticationError, ServerLocker

from helpers import LockerTestCase, PASSWORD, run_child, wait_until

NEEDS_MSGPACK = unittest.skipUnless(lockerModule.MSGPACK_AVAILABLE, 'needs the msgpack package')


class TestSerializerChoice(LockerTestCase):
    """Checks which serializer each locker ends up using."""

    def test_json_is_the_default(self):
        """Uses JSON when no serializer is asked for."""
        server, client = self.make_pair()
        self.assertFalse(server._useMsgpack)
        self.assertFalse(client._useMsgpack)

    @NEEDS_MSGPACK
    def test_client_follows_a_msgpack_server(self):
        """Switches a client that asked for nothing, or for JSON, to the msgpack of the server."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile, serializer='msgpack')
        plain = self.make_client(serverFile)
        asked = self.make_client(serverFile, serializer='msgpack')
        self.assertTrue(server._useMsgpack)
        self.assertTrue(plain._useMsgpack)
        self.assertTrue(asked._useMsgpack)

    @NEEDS_MSGPACK
    def test_client_follows_a_json_server(self):
        """Uses JSON even when the client asked for msgpack, because the server decides."""
        serverFile = self.server_file()
        self.make_locker(serverFile)
        client = self.make_client(serverFile, serializer='msgpack')
        self.assertFalse(client._useMsgpack)
        self.assertTrue(client.acquire_lock(self.lock_path('x'), timeout=2)[0])

    def test_missing_msgpack_falls_back_to_json_with_a_warning(self):
        """Uses JSON and warns when msgpack is asked for and the package is missing."""
        logger = logging.getLogger('pylocker.test.serializers')
        with mock.patch.object(lockerModule, 'MSGPACK_AVAILABLE', False):
            with self.assertLogs(logger, level='WARNING') as captured:
                locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False,
                                      serializer='msgpack', logger=logger)
        self.assertFalse(locker._useMsgpack)
        self.assertTrue(any('falling back to JSON' in line for line in captured.output))

    def test_unknown_serializer_is_refused(self):
        """Raises ValueError for a serializer that does not exist."""
        with self.assertRaises(ValueError):
            ServerLocker(PASSWORD, serverFile=False, autoconnect=False, serializer='xml')


@NEEDS_MSGPACK
class TestMsgpackTraffic(LockerTestCase):
    """Checks that everything a locker does works over msgpack."""

    def setUp(self):
        """Creates a msgpack server and two clients."""
        super().setUp()
        serverFile = self.server_file()
        self.server = self.make_locker(serverFile, serializer='msgpack', allowRemoteOrders=True)
        self.first = self.make_client(serverFile)
        self.second = self.make_client(serverFile)

    def test_locking(self):
        """Locks, refuses a second holder, and locks again after the release."""
        path = self.lock_path('mp')
        acquired, lockId = self.first.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.assertEqual(self.second.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        self.first.release_lock(lockId)
        self.assertTrue(self.second.acquire_lock(path, timeout=3)[0])

    def test_messages(self):
        """Delivers a message with a timeout to every locker, and removes it when the time is over."""
        self.first.publish_message('mpMessage', timeout=1)
        self.assertTrue(wait_until(lambda: self.second.has_message('mpMessage')))
        self.assertTrue(self.server.has_message('mpMessage'))
        self.assertTrue(wait_until(lambda: not self.second.has_message('mpMessage'), seconds=4))

    def test_stop_order(self):
        """Carries a stop order from a client to the server."""
        self.first.stop_remote(password=PASSWORD)
        self.assertTrue(wait_until(lambda: not self.server.isServer))

    def test_client_in_another_process(self):
        """Lets a client in another process lock through the msgpack server."""
        code = f"""
        client = ServerLocker(PASSWORD, serverFile={self.server.serverFile!r}, connectTimeout=5, allowServing=False)
        print(client._useMsgpack, client.acquire_lock({self.lock_path('other')!r}, timeout=3)[0])
        client.stop()
        """
        completed = run_child(code)
        self.assertEqual(completed.stdout.strip().splitlines()[-1], 'True True', completed.stderr[-300:])

    def test_wrong_password_is_refused(self):
        """Refuses a wrong password in the JSON handshake that comes before msgpack."""
        with self.assertRaises(AuthenticationError):
            ServerLocker('WRONG', serverFile=self.server.serverFile, connectTimeout=5, allowServing=False)


if __name__ == '__main__':
    unittest.main()

"""Tests that a request too big to send is refused to the caller and never costs the locker its connection."""
import json
import socket
import time
import unittest
import uuid

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, login_by_hand


class TestRequestSize(LockerTestCase):
    """Checks the size limits on what a caller can ask for."""

    def test_big_path_list_is_refused_and_the_connection_survives(self):
        """Raises ValueError for paths that would be above the message limit, and keeps working afterwards."""
        _, client = self.make_pair()
        paths = [f'{index}_' + 'x' * 100 for index in range(900)]
        with self.assertRaisesRegex(ValueError, 'once encoded'):
            client.acquire_lock(paths, timeout=2, retries=0)
        self.assertTrue(client.isClient)
        acquired, _ = client.acquire_lock('after', timeout=2, retries=0)
        self.assertTrue(acquired)

    def test_async_acquire_refuses_the_same_request(self):
        """Raises ValueError from acquire_async too."""
        import asyncio
        _, client = self.make_pair()
        paths = [f'{index}_' + 'x' * 100 for index in range(900)]
        with self.assertRaises(ValueError):
            asyncio.run(client.acquire_async(paths, timeout=2))
        self.assertTrue(client.isClient)

    def test_many_short_paths_are_still_accepted(self):
        """Grants a request of 900 short paths, which fits in a message."""
        _, client = self.make_pair()
        acquired, _ = client.acquire_lock([f'p{index}' for index in range(900)], timeout=5, retries=0)
        self.assertTrue(acquired)

    def test_path_that_is_too_long_is_refused(self):
        """Raises ValueError for a path above the length limit and accepts one at the limit."""
        _, client = self.make_pair()
        with self.assertRaisesRegex(ValueError, 'at most'):
            client.acquire_lock('p' * (lockerModule._MAX_PATH_LENGTH + 1), timeout=2, retries=0)
        acquired, _ = client.acquire_lock('p' * lockerModule._MAX_PATH_LENGTH, timeout=2, retries=0)
        self.assertTrue(acquired)

    def test_server_ignores_a_path_that_is_too_long(self):
        """Drops a request with a long path sent by a program that skips the client checks, and keeps its connection."""
        server, _ = self.make_pair()
        messages = []
        server._warn = messages.append
        raw = socket.create_connection(('127.0.0.1', server._serverPort), timeout=3)
        reader = raw.makefile('rb')
        self.addCleanup(raw.close)
        self.addCleanup(reader.close)
        self.assertEqual(login_by_hand(raw, reader, uniqueName='raw')['action'], 'welcome')
        longPath = 'p' * (lockerModule._MAX_PATH_LENGTH + 1)
        raw.sendall(json.dumps({'action': 'acquire', 'request_unique_id': str(uuid.uuid4()), 'path': [longPath],
                                'timeout': 5}).encode('utf-8') + b'\n')
        time.sleep(0.5)
        self.assertTrue(any('at most' in message for message in messages))
        raw.sendall(json.dumps({'action': 'acquire', 'request_unique_id': 'ok-1', 'path': ['fine'],
                                'timeout': 5}).encode('utf-8') + b'\n')
        self.assertEqual(json.loads(reader.readline())['action'], 'acquired')

    def test_publish_with_too_many_receivers_is_refused(self):
        """Raises ValueError for receivers that make the message too big, and keeps the connection."""
        _, client = self.make_pair()
        receivers = [str(uuid.uuid4()) for _ in range(3000)]
        with self.assertRaisesRegex(ValueError, 'receivers'):
            client.publish_message('hello', receivers=receivers)
        self.assertTrue(client.isClient)
        self.assertTrue(client.publish_message('hello', receivers=[str(uuid.uuid4())])[0])

    def test_stop_remote_with_too_many_receivers_is_refused(self):
        """Raises ValueError for receivers that make the order too big, and keeps the connection."""
        _, client = self.make_pair()
        receivers = [str(uuid.uuid4()) for _ in range(3000)]
        with self.assertRaisesRegex(ValueError, 'receivers'):
            client.stop_remote('pw', receivers=receivers)
        self.assertTrue(client.isClient)


if __name__ == '__main__':
    unittest.main()

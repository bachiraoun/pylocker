"""Tests callers that give up, clients whose clock is wrong, the lock index, the host name lookup and message frames."""
import asyncio
import struct
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, wait_until
from test_hostile import RawClient, acquire_request


class TestCallerThatGivesUp(LockerTestCase):
    """Checks that a request whose caller was cancelled is never left holding a lock."""

    def cancel_after(self, locker, path, seconds):
        """Starts an asynchronous acquire in a new event loop and cancels it after a short wait.

        :Parameters:
            #. locker (ServerLocker): The locker that acquires.
            #. path (str): The path to lock.
            #. seconds (float): How long the caller waits before it is cancelled.
        """
        async def acquire_and_give_up():
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(locker.acquire_async(path, timeout=20), seconds)
        asyncio.run(acquire_and_give_up())

    def test_cancelled_client_request_does_not_keep_the_lock(self):
        """Frees the path for the next caller when the client that wanted it was cancelled while waiting."""
        server, client = self.make_pair()
        path = self.lock_path('busy')
        holdId = server.acquire_lock(path, timeout=5)[1]
        self.cancel_after(client, path, 0.5)
        server.release_lock(holdId)
        self.assertTrue(wait_until(lambda: path not in server.lockedPaths, seconds=5))
        self.assertTrue(server.acquire_lock(path, timeout=2, retries=0)[0])

    def test_cancelled_server_request_does_not_keep_the_lock(self):
        """Frees the path when the server's own caller was cancelled while waiting."""
        server, client = self.make_pair()
        path = self.lock_path('busy')
        holdId = client.acquire_lock(path, timeout=5)[1]
        self.cancel_after(server, path, 0.5)
        client.release_lock(holdId)
        self.assertTrue(wait_until(lambda: path not in server.lockedPaths, seconds=5))
        self.assertEqual(sum(server._pendingCounts.values()), 0)
        self.assertTrue(client.acquire_lock(path, timeout=2, retries=0)[0])

    def test_cancelled_request_leaves_no_pending_future(self):
        """Forgets the waiting request of a cancelled client call."""
        server, client = self.make_pair()
        path = self.lock_path('busy')
        server.acquire_lock(path, timeout=5)
        self.cancel_after(client, path, 0.3)
        self.assertEqual(len(client._pending), 0)


class TestClockOfTheClient(LockerTestCase):
    """Checks that the server's clock, not the client's, decides when a request expires."""

    def setUp(self):
        """Creates a server and a hand-made client."""
        super().setUp()
        self.server = self.make_locker(self.server_file())
        self.raw = RawClient(self.server._serverPort)
        self.addCleanup(self.raw.close)

    def test_client_clock_far_behind_still_gets_a_free_path(self):
        """Grants a free path to a request that claims it was made long ago."""
        self.raw.send(acquire_request(path=[self.lock_path('free')], timeout=5, request_utctime=time.time() - 1.0e6))
        self.assertEqual(self.raw.read()['action'], 'acquired')

    def test_client_clock_far_ahead_does_not_make_a_request_immortal(self):
        """Drops a waiting request after its timeout even when it claims it was made in the future."""
        path = self.lock_path('busy')
        self.server.acquire_lock(path, timeout=5)
        self.raw.send(acquire_request(path=[path], timeout=1, request_utctime=time.time() + 1.0e6))
        self.assertTrue(wait_until(lambda: sum(self.server._pendingCounts.values()) == 1))
        self.assertTrue(wait_until(lambda: sum(self.server._pendingCounts.values()) == 0, seconds=5))


class NoScanDict(dict):
    """A dictionary that fails when someone walks over all of its entries."""

    def items(self):
        """Fails, because walking over all locks is what a release must not do."""
        raise AssertionError('the lock table was scanned')


class TestLockIndex(LockerTestCase):
    """Checks that releasing a lock does not walk over every lock of the server."""

    def test_release_does_not_scan_the_lock_table(self):
        """Releases a lock, and ignores a release of an unknown lock, without walking over the table."""
        server, client = self.make_pair()
        path = self.lock_path('held')
        lockId = client.acquire_lock(path, timeout=5)[1]
        server._pathsLUT = NoScanDict(server._pathsLUT)
        self.addCleanup(setattr, server, '_pathsLUT', dict(server._pathsLUT))
        raw = RawClient(server._serverPort)
        self.addCleanup(raw.close)
        raw.send({'action': 'release', 'request_unique_id': 'made-up', 'path': []})
        client.release_lock(lockId)
        self.assertTrue(wait_until(lambda: path not in server._pathsLUT))

    def test_index_follows_the_locks(self):
        """Holds an entry for each lock request while it is held, and none after it is released or evicted."""
        server, client = self.make_pair(maxLockTime=1)
        path = self.lock_path('held')
        lockId = client.acquire_lock([path, path + '2'], timeout=5)[1]
        self.assertEqual(server._locksByRequest[lockId], {path, path + '2'})
        client.release_lock(lockId)
        self.assertTrue(wait_until(lambda: len(server._locksByRequest) == 0))
        lockId = client.acquire_lock(path, timeout=5)[1]
        self.assertTrue(wait_until(lambda: len(server._locksByRequest) == 0, seconds=12))

    def test_index_is_cleared_when_the_client_leaves(self):
        """Forgets the locks of a client that disconnected."""
        server, client = self.make_pair()
        client.acquire_lock(self.lock_path('held'), timeout=5)
        client.stop()
        self.assertTrue(wait_until(lambda: len(server._locksByRequest) == 0))

    def test_other_connection_cannot_release_the_lock(self):
        """Keeps a lock that another connection tries to release, and keeps its index entry."""
        server, client = self.make_pair()
        path = self.lock_path('held')
        lockId = client.acquire_lock(path, timeout=5)[1]
        raw = RawClient(server._serverPort)
        self.addCleanup(raw.close)
        raw.send({'action': 'release', 'request_unique_id': lockId, 'path': [path]})
        time.sleep(0.5)
        self.assertIn(path, server._pathsLUT)
        self.assertIn(lockId, server._locksByRequest)


class TestHostNameLookup(LockerTestCase):
    """Checks that a slow host name lookup does not hold up the server."""

    def test_slow_lookup_does_not_delay_lock_requests(self):
        """Serves lock requests at once while the dead-process monitor waits for the host name."""
        def slow_lookup(_name):
            time.sleep(1.5)
            return '127.0.0.1'
        with mock.patch.object(lockerModule, '_DEAD_PID_POLL', 0.2), \
                mock.patch.object(lockerModule.socket, 'gethostbyname', slow_lookup):
            server, client = self.make_pair()
            time.sleep(0.6)
            slowest = 0.0
            for index in range(5):
                start = time.time()
                self.assertTrue(client.acquire_lock(self.lock_path(f'p{index}'), timeout=5)[0])
                slowest = max(slowest, time.time() - start)
            self.assertLess(slowest, 0.5)

    def test_local_addresses_hold_the_loopback_address(self):
        """Lists the loopback address and survives a host name that does not resolve."""
        server = self.make_locker(self.server_file())
        with mock.patch.object(lockerModule.socket, 'gethostbyname', side_effect=OSError('no name')):
            self.assertIn('127.0.0.1', server._local_addresses())


@unittest.skipUnless(lockerModule.MSGPACK_AVAILABLE, 'needs the msgpack package')
class TestMsgpackFrames(unittest.TestCase):
    """Checks how the msgpack reader treats a wait that ends before, or in the middle of, a frame."""

    @staticmethod
    def frame(content):
        """Builds a length-prefixed msgpack frame.

        :Parameters:
            #. content (dict): The message.

        :Returns:
            #. frame (bytes): The header and the payload.
        """
        payload = lockerModule._msgpack.packb(content, use_bin_type=True)
        return struct.pack('>I', len(payload)) + payload

    def test_idle_timeout_keeps_a_partly_arrived_header(self):
        """Loses nothing when the wait ends after only part of the header arrived."""
        async def read():
            reader = asyncio.StreamReader()
            data = self.frame({'action': 'x'})
            reader.feed_data(data[:2])
            with self.assertRaises(asyncio.TimeoutError):
                await lockerModule._read_msgpack(reader, 1000, idleTimeout=0.2)
            reader.feed_data(data[2:])
            return await lockerModule._read_msgpack(reader, 1000, idleTimeout=1)
        self.assertEqual(asyncio.run(read()), {'action': 'x'})

    def test_frame_that_does_not_end_closes_the_connection(self):
        """Raises a connection error when the payload of a frame does not arrive in time."""
        async def read():
            reader = asyncio.StreamReader()
            reader.feed_data(self.frame({'action': 'x'})[:6])
            with mock.patch.object(lockerModule, '_FRAME_BODY_SECONDS', 0.2):
                await lockerModule._read_msgpack(reader, 1000, idleTimeout=1)
        # The outer limit makes a reader without its own deadline fail the test instead of hanging it
        with self.assertRaises(ConnectionError):
            asyncio.run(asyncio.wait_for(read(), timeout=5))


if __name__ == '__main__':
    unittest.main()

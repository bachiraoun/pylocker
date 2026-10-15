"""Tests acquiring and releasing locks between a server and a client."""
import asyncio
import time
import threading

from helpers import LockerTestCase


class TestSyncLocking(LockerTestCase):
    """Checks the blocking acquire and release."""

    def setUp(self):
        """Creates a server and a client."""
        super().setUp()
        self.server, self.client = self.make_pair()

    def test_path_shapes(self):
        """Locks a path given as a string, a list or a tuple."""
        for label, path in (('string', self.lock_path('s')), ('list', [self.lock_path('l1'), self.lock_path('l2')]),
                            ('tuple', (self.lock_path('t'),))):
            with self.subTest(label):
                acquired, lockId = self.client.acquire_lock(path, timeout=2)
                self.assertTrue(acquired)
                self.assertEqual(self.client.release_lock(lockId), (True, 1))

    def test_contended_path_times_out(self):
        """Returns (False, 0) when another locker holds the path."""
        path = self.lock_path('busy')
        acquired, lockId = self.client.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.assertEqual(self.server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        self.client.release_lock(lockId)

    def test_released_path_can_be_locked_again(self):
        """Grants a path again once its holder released it."""
        path = self.lock_path('again')
        acquired, lockId = self.server.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.server.release_lock(lockId)
        self.assertTrue(self.client.acquire_lock(path, timeout=2)[0])

    def test_all_paths_of_a_request_are_locked_together(self):
        """Refuses a request when one of its paths is held."""
        held = self.lock_path('held')
        self.assertTrue(self.server.acquire_lock(held, timeout=2)[0])
        result = self.client.acquire_lock([self.lock_path('free'), held], timeout=0.3, retries=0)
        self.assertEqual(result, (False, 0))
        self.assertTrue(self.client.acquire_lock(self.lock_path('free'), timeout=1)[0])

    def test_arguments(self):
        """Accepts a default timeout, a float timeout and positional arguments."""
        for label, call in (('default timeout', lambda p: self.client.acquire_lock(p)),
                            ('float timeout', lambda p: self.client.acquire_lock(p, timeout=1.5)),
                            ('positional', lambda p: self.client.acquire_lock(p, 2, 1, 0.1))):
            with self.subTest(label):
                acquired, lockId = call(self.lock_path(label))
                self.assertTrue(acquired)
                self.client.release_lock(lockId)

    def test_timeout_is_kept(self):
        """Gives up after about the timeout when the path stays held."""
        path = self.lock_path('wait')
        self.assertTrue(self.server.acquire_lock(path, timeout=2)[0])
        start = time.time()
        result = self.client.acquire_lock(path, timeout=0.5, retries=0)
        self.assertEqual(result, (False, 0))
        self.assertTrue(0.4 < time.time() - start < 1.8)

    def test_retries_are_kept(self):
        """Tries again and waits between attempts."""
        path = self.lock_path('retry')
        self.assertTrue(self.server.acquire_lock(path, timeout=2)[0])
        start = time.time()
        result = self.client.acquire_lock(path, timeout=0.4, retries=2, retryDelay=0.2)
        self.assertEqual(result, (False, 0))
        self.assertTrue(1.4 < time.time() - start < 3.5)

    def test_release_of_unknown_lock(self):
        """Reports a lock that is already released as released."""
        self.assertEqual(self.client.release_lock('not-a-lock-id'), (True, 0))

    def test_context_manager(self):
        """Locks inside a with block and frees the path afterwards."""
        path = self.lock_path('with')
        with self.client.lock(path, timeout=2) as (acquired, lockId):
            self.assertTrue(acquired)
            self.assertEqual(self.server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        self.assertTrue(self.server.acquire_lock(path, timeout=3)[0])

    def test_threads_never_hold_a_path_together(self):
        """Lets only one of many threads hold a path at a time."""
        path = self.lock_path('shared')
        holders, overlaps = [], []
        counterLock = threading.Lock()

        def work(locker):
            """Takes the path ten times and records any overlap."""
            for _ in range(10):
                acquired, lockId = locker.acquire_lock(path, timeout=10)
                if not acquired:
                    continue
                with counterLock:
                    holders.append(1)
                    if len(holders) > 1:
                        overlaps.append(1)
                time.sleep(0.002)
                with counterLock:
                    holders.pop()
                locker.release_lock(lockId)

        threads = [threading.Thread(target=work, args=(self.client if index % 2 else self.server,))
                   for index in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.assertEqual(overlaps, [])


class TestAsyncLocking(LockerTestCase):
    """Checks the asyncio acquire and release."""

    def test_acquire_release_and_contention(self):
        """Locks, refuses a second holder and locks again after the release, from a client and a server."""
        server, client = self.make_pair()
        path = self.lock_path('a1')

        async def scenario():
            """Runs the steps on the caller's event loop."""
            acquired, lockId = await client.acquire_async(path, timeout=2)
            self.assertTrue(acquired)
            self.assertFalse((await client.acquire_async(path, timeout=0.3))[0])
            self.assertTrue((await client.release_async(lockId))[0])
            acquired, lockId = await client.acquire_async(path, timeout=2)
            self.assertTrue(acquired)
            await client.release_async(lockId)
            acquired, lockId = await server.acquire_async([self.lock_path('a2'), self.lock_path('a3')])
            self.assertTrue(acquired)
            await server.release_async(lockId)

        asyncio.run(scenario())

    def test_async_context_manager(self):
        """Locks inside an async with block."""
        server, client = self.make_pair()
        path = self.lock_path('actx')

        async def scenario():
            """Runs the steps on the caller's event loop."""
            async with client.lock(path, timeout=2) as (acquired, lockId):
                self.assertTrue(acquired)
                self.assertFalse((await server.acquire_async(path, timeout=0.3))[0])

        asyncio.run(scenario())
        self.assertTrue(server.acquire_lock(path, timeout=3)[0])

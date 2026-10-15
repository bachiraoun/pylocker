"""Tests lockers competing for the same paths: processes, threads and asyncio tasks, queue order, expiry and eviction."""
import asyncio
import json
import logging
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, spawn_child, wait_until

SERIALIZERS = ('json', 'msgpack')

MUTUAL_EXCLUSION_WORKER = """
import json
locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=10, allowServing=False)
rounds = overlaps = timeouts = 0
for _ in range({rounds}):
    acquired, lockId = locker.acquire_lock({path!r}, timeout=60, retries=0)
    if not acquired:
        timeouts += 1
        continue
    try:
        descriptor = os.open({marker!r}, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(descriptor)
    except FileExistsError:
        overlaps += 1
    else:
        time.sleep(0.001)
        os.unlink({marker!r})
    rounds += 1
    locker.release_lock(lockId)
print(json.dumps({{'rounds': rounds, 'overlaps': overlaps, 'timeouts': timeouts}}))
locker.stop()
"""

PATH_SET_WORKER = """
import json
locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=10, allowServing=False)
paths = {paths!r}
rounds = overlaps = timeouts = 0
for _ in range({rounds}):
    acquired, lockId = locker.acquire_lock(paths, timeout=60, retries=0)
    if not acquired:
        timeouts += 1
        continue
    created = []
    for path in paths:
        try:
            descriptor = os.open(path + '.marker', os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(descriptor)
            created.append(path + '.marker')
        except FileExistsError:
            overlaps += 1
    time.sleep(0.001)
    for marker in created:
        os.unlink(marker)
    rounds += 1
    locker.release_lock(lockId)
print(json.dumps({{'rounds': rounds, 'overlaps': overlaps, 'timeouts': timeouts}}))
locker.stop()
"""


def serializer_is_available(serializer):
    """Tells whether a serializer can be used on this machine.

    :Parameters:
        #. serializer (str): ``json`` or ``msgpack``.

    :Returns:
        #. isAvailable (bool): False only for msgpack when the package is not installed.
    """
    return serializer == 'json' or lockerModule.MSGPACK_AVAILABLE


class TestProcessContention(LockerTestCase):
    """Checks that processes competing for paths never hold one together and never wait for ever."""

    PROCESSES = 8
    ROUNDS = 60

    def run_workers(self, serverFile, template, **fields):
        """Runs several worker processes to the end and adds up what they report.

        :Parameters:
            #. serverFile (str): The server file the workers connect to.
            #. template (str): The worker code, with the fields as ``{name}`` markers.
            #. fields (dict): Values for the markers; a list value gives one worker per item.

        :Returns:
            #. totals (dict): The sums of ``rounds``, ``overlaps`` and ``timeouts``.
        """
        variants = fields.pop('variants')
        children = [spawn_child(template.format(serverFile=serverFile, rounds=self.ROUNDS, **fields, **variant))
                    for variant in variants]
        totals = {'rounds': 0, 'overlaps': 0, 'timeouts': 0}
        for child in children:
            output, _ = child.communicate(timeout=180)
            self.assertEqual(child.returncode, 0)
            report = json.loads(output.strip().splitlines()[-1])
            for name in totals:
                totals[name] += report[name]
        return totals

    def test_one_path_is_never_held_twice(self):
        """Lets only one process at a time hold a path, with each serializer."""
        for serializer in SERIALIZERS:
            with self.subTest(serializer=serializer):
                if not serializer_is_available(serializer):
                    self.skipTest('msgpack is not installed')
                serverFile = self.server_file(f'{serializer}.lock')
                self.make_locker(serverFile, serializer=serializer)
                path = self.lock_path(f'shared_{serializer}')
                totals = self.run_workers(serverFile, MUTUAL_EXCLUSION_WORKER, path=path,
                                          marker=path + '.marker',
                                          variants=[{} for _ in range(self.PROCESSES)])
                self.assertEqual(totals['overlaps'], 0)
                self.assertEqual(totals['timeouts'], 0)
                self.assertEqual(totals['rounds'], self.PROCESSES * self.ROUNDS)

    def test_overlapping_path_sets_do_not_deadlock(self):
        """Lets processes lock [a, b] and [b, a] over and over without a deadlock or a shared hold."""
        serverFile = self.server_file()
        self.make_locker(serverFile)
        first, second = self.lock_path('a'), self.lock_path('b')
        variants = [{'paths': [first, second]}, {'paths': [second, first]},
                    {'paths': [first, second]}, {'paths': [second, first]}]
        totals = self.run_workers(serverFile, PATH_SET_WORKER, variants=variants)
        self.assertEqual(totals['overlaps'], 0)
        self.assertEqual(totals['timeouts'], 0)
        self.assertEqual(totals['rounds'], len(variants) * self.ROUNDS)

    def test_disjoint_paths_do_not_wait_for_each_other(self):
        """Lets processes that lock different paths finish without any timeout."""
        serverFile = self.server_file()
        self.make_locker(serverFile)
        variants = [{'paths': [self.lock_path(f'own{index}')]} for index in range(self.PROCESSES)]
        totals = self.run_workers(serverFile, PATH_SET_WORKER, variants=variants)
        self.assertEqual((totals['overlaps'], totals['timeouts']), (0, 0))
        self.assertEqual(totals['rounds'], self.PROCESSES * self.ROUNDS)


class TestThreadAndTaskContention(LockerTestCase):
    """Checks competition inside one process, from threads and from asyncio tasks."""

    def test_async_tasks_never_hold_a_path_together(self):
        """Lets fifty tasks, spread over a client and a server, take one path in turn."""
        server, client = self.make_pair()
        path = self.lock_path('tasks')

        async def scenario():
            """Runs the competing tasks and counts overlaps and failures."""
            state = {'inside': 0, 'overlaps': 0, 'failures': 0, 'rounds': 0}

            async def work(locker):
                """Takes and frees the path three times."""
                for _ in range(3):
                    acquired, lockId = await locker.acquire_async(path, timeout=60)
                    if not acquired:
                        state['failures'] += 1
                        continue
                    state['inside'] += 1
                    if state['inside'] > 1:
                        state['overlaps'] += 1
                    await asyncio.sleep(0.001)
                    state['inside'] -= 1
                    state['rounds'] += 1
                    await locker.release_async(lockId)

            await asyncio.gather(*[work(client if index % 2 else server) for index in range(50)])
            return state

        state = asyncio.run(scenario())
        self.assertEqual(state['overlaps'], 0)
        self.assertEqual(state['failures'], 0)
        self.assertEqual(state['rounds'], 150)

    def test_thirty_clients_on_the_shared_loop(self):
        """Lets thirty clients of one process, which share one event loop, take a path in turn."""
        serverFile = self.server_file()
        self.make_locker(serverFile)
        clients = [self.make_client(serverFile) for _ in range(30)]
        self.assertTrue(all(client._usesSharedLoop for client in clients))
        path = self.lock_path('shared_loop')
        counter = threading.Lock()
        state = {'inside': 0, 'overlaps': 0, 'failures': 0, 'rounds': 0}

        def work(client):
            """Takes and frees the path five times."""
            for _ in range(5):
                acquired, lockId = client.acquire_lock(path, timeout=60, retries=0)
                if not acquired:
                    with counter:
                        state['failures'] += 1
                    continue
                with counter:
                    state['inside'] += 1
                    if state['inside'] > 1:
                        state['overlaps'] += 1
                time.sleep(0.001)
                with counter:
                    state['inside'] -= 1
                    state['rounds'] += 1
                client.release_lock(lockId)

        threads = [threading.Thread(target=work, args=(client,)) for client in clients]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(120)
        self.assertEqual((state['overlaps'], state['failures']), (0, 0))
        self.assertEqual(state['rounds'], 150)


class TestQueue(LockerTestCase):
    """Checks how the server queues, expires and evicts lock requests."""

    def test_waiters_are_served_in_arrival_order(self):
        """Gives a freed path to the waiters in the order they asked."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        clients = [self.make_client(serverFile) for _ in range(3)]
        path = self.lock_path('fifo')
        acquired, holdId = server.acquire_lock(path, timeout=5)
        self.assertTrue(acquired)
        order = []

        def wait_for_lock(index, client):
            """Waits for the path, notes the turn, and gives the path back."""
            acquired, lockId = client.acquire_lock(path, timeout=30, retries=0)
            order.append(index)
            time.sleep(0.05)
            if acquired:
                client.release_lock(lockId)

        threads = []
        for index, client in enumerate(clients):
            thread = threading.Thread(target=wait_for_lock, args=(index, client))
            thread.start()
            threads.append(thread)
            time.sleep(0.3)
        server.release_lock(holdId)
        for thread in threads:
            thread.join(30)
        self.assertEqual(order, [0, 1, 2])

    def test_expired_request_is_dropped(self):
        """Never gives a path to a request whose timeout ran out while it waited, and says so in the log."""
        logger = logging.getLogger('pylocker.test.expire')
        server, client = self.make_pair(logger=logger)
        path = self.lock_path('expire')
        acquired, holdId = server.acquire_lock(path, timeout=5)
        self.assertTrue(acquired)
        start = time.time()
        with self.assertLogs(logger, level='WARNING') as captured:
            self.assertEqual(client.acquire_lock(path, timeout=0.5, retries=0), (False, 0))
            self.assertTrue(0.4 < time.time() - start < 2)
            server.release_lock(holdId)
            time.sleep(0.3)
        self.assertTrue(any('expired in queue' in line for line in captured.output))
        self.assertEqual(server.lockedPaths, {})
        acquired, _ = server.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.assertEqual(len(server.lockedPaths), 1)

    def test_lock_held_too_long_is_evicted(self):
        """Takes a lock back after maxLockTime and gives the path to the next waiter."""
        with mock.patch.object(lockerModule, '_MONITOR_POLL', 0.3):
            serverFile = self.server_file()
            server = self.make_locker(serverFile, maxLockTime=1.0)
            client = self.make_client(serverFile)
            path = self.lock_path('slow')
            acquired, staleId = client.acquire_lock(path, timeout=2)
            self.assertTrue(acquired)
            start = time.time()
            acquired, _ = server.acquire_lock(path, timeout=10, retries=0)
            seconds = time.time() - start
        self.assertTrue(acquired)
        self.assertTrue(0.5 < seconds < 3.5, seconds)
        # Releasing the evicted lock must not free the path its new holder now has.
        client.release_lock(staleId)
        time.sleep(0.3)
        self.assertEqual(len(server.lockedPaths), 1)

    def test_evicted_holder_is_told(self):
        """Removes an evicted lock from the holder's own list, because the server tells the holder."""
        with mock.patch.object(lockerModule, '_MONITOR_POLL', 0.3):
            serverFile = self.server_file()
            server = self.make_locker(serverFile, maxLockTime=1.0)
            client = self.make_client(serverFile)
            path = self.lock_path('told')
            self.assertTrue(client.acquire_lock(path, timeout=2)[0])
            self.assertTrue(server.acquire_lock(path, timeout=10, retries=0)[0])
        self.assertTrue(wait_until(lambda: client.ownedLocks == {}, seconds=3))

    def test_evicted_multi_path_lock_is_told_once(self):
        """Removes a lock on several paths from the holder's list when it is evicted."""
        with mock.patch.object(lockerModule, '_MONITOR_POLL', 0.3):
            serverFile = self.server_file()
            server = self.make_locker(serverFile, maxLockTime=1.0)
            client = self.make_client(serverFile)
            paths = [self.lock_path('m1'), self.lock_path('m2')]
            acquired, lockId = client.acquire_lock(paths, timeout=2)
            self.assertTrue(acquired)
            self.assertEqual(list(client.ownedLocks), [lockId])
            self.assertTrue(server.acquire_lock(paths, timeout=10, retries=0)[0])
        self.assertTrue(wait_until(lambda: lockId not in client.ownedLocks, seconds=3))

    def test_evicted_lock_of_the_server_leaves_its_own_list(self):
        """Removes an evicted lock from the server's own list when the server was the holder."""
        with mock.patch.object(lockerModule, '_MONITOR_POLL', 0.3):
            serverFile = self.server_file()
            server = self.make_locker(serverFile, maxLockTime=1.0)
            client = self.make_client(serverFile)
            path = self.lock_path('own')
            acquired, lockId = server.acquire_lock(path, timeout=2)
            self.assertTrue(acquired)
            self.assertIn(lockId, server.ownedLocks)
            self.assertTrue(client.acquire_lock(path, timeout=10, retries=0)[0])
        self.assertTrue(wait_until(lambda: lockId not in server.ownedLocks, seconds=3))

    def test_eviction_does_not_disturb_a_client_that_left(self):
        """Evicts the lock of a client that already disconnected without an error."""
        with mock.patch.object(lockerModule, '_MONITOR_POLL', 0.3):
            serverFile = self.server_file()
            server = self.make_locker(serverFile, maxLockTime=1.0)
            client = self.make_client(serverFile)
            path = self.lock_path('left')
            self.assertTrue(client.acquire_lock(path, timeout=2)[0])
            client._loop.call_soon_threadsafe(client._writer.transport.abort)
            self.assertTrue(server.acquire_lock(path, timeout=10, retries=0)[0])
            self.assertTrue(server.isServer)

    def test_max_lock_time_does_not_touch_short_holds(self):
        """Leaves a lock alone while it is younger than maxLockTime."""
        with mock.patch.object(lockerModule, '_MONITOR_POLL', 0.2):
            server, client = self.make_pair(maxLockTime=30)
            path = self.lock_path('young')
            self.assertTrue(client.acquire_lock(path, timeout=2)[0])
            time.sleep(1.0)
            self.assertEqual(server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))

    def test_released_lock_wakes_the_next_waiter_quickly(self):
        """Hands a freed path to a waiter within a fraction of a second."""
        server, client = self.make_pair()
        path = self.lock_path('wake')
        acquired, holdId = server.acquire_lock(path, timeout=5)
        self.assertTrue(acquired)
        result = {}

        def wait():
            """Waits for the path and notes when it was granted."""
            result['acquired'] = client.acquire_lock(path, timeout=10, retries=0)[0]
            result['time'] = time.time()

        thread = threading.Thread(target=wait)
        thread.start()
        time.sleep(0.5)
        released = time.time()
        server.release_lock(holdId)
        thread.join(15)
        self.assertTrue(result['acquired'])
        self.assertLess(result['time'] - released, 1.0)


if __name__ == '__main__':
    unittest.main()

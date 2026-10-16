"""Tests the caps on locks, publications and logins in progress, and the errors that are logged instead of hidden."""
import logging
import select
import socket
import time
import unittest
from unittest import mock

from pylocker import ServerLocker
from pylocker import Locker as lockerModule

from helpers import FakeServer, LockerTestCase, PASSWORD, wait_until
from test_hostile import RawClient, acquire_request
from test_publish_validation import publication


class TestLocksPerClient(LockerTestCase):
    """Checks the limit on the paths one client may hold locked."""

    def setUp(self):
        """Creates a server that lets a client hold five paths, and a hand-made client."""
        super().setUp()
        self.server = self.make_locker(self.server_file(), maxLocksPerClient=5)
        self.raw = RawClient(self.server._serverPort)
        self.addCleanup(self.raw.close)

    def request(self, name, paths=None):
        """Sends an acquire request for one new path, or for the given paths.

        :Parameters:
            #. name (str): Name of the request and of its path.
            #. paths (None, list): The paths to ask for, one path named like the request when None.
        """
        self.raw.send(acquire_request(request_unique_id=name, path=paths or [f'cap/{name}'], timeout=20))

    def granted(self, wait=0.6):
        """Tells whether the hand-made client receives a grant within a short time.

        :Parameters:
            #. wait (int, float): Seconds to wait.

        :Returns:
            #. name (None, str): The request id of the grant, None when nothing came.
        """
        # A read that timed out leaves a buffered file object unusable, so readiness is checked first
        isReady, _, _ = select.select([self.raw.connection], [], [], wait)
        return self.raw.read()['request_unique_id'] if isReady else None

    def test_request_over_the_limit_waits_until_a_lock_is_released(self):
        """Holds back the sixth path, and grants it when the client releases one."""
        for index in range(5):
            self.request(f'r{index}')
            self.assertEqual(self.granted(), f'r{index}')
        self.request('r5')
        self.assertIsNone(self.granted())
        self.raw.send({'action': 'release', 'request_unique_id': 'r0'})
        self.assertEqual(self.granted(3), 'r5')

    def test_request_for_more_paths_than_the_limit_is_never_granted(self):
        """Never grants one request for six paths, and does not let it take paths from others."""
        self.request('wide', [f'cap/wide/{index}' for index in range(6)])
        self.assertIsNone(self.granted(1))
        self.assertEqual(len(self.server._pathsLUT), 0)

    def test_count_goes_back_to_zero(self):
        """Counts no locks for a client that released them, was evicted, or left."""
        self.request('a')
        self.assertEqual(self.granted(), 'a')
        self.assertEqual(sum(self.server._heldCounts.values()), 1)
        self.raw.send({'action': 'release', 'request_unique_id': 'a'})
        self.assertTrue(wait_until(lambda: self.server._heldCounts == {}))
        self.request('b')
        self.assertEqual(self.granted(), 'b')
        self.raw.close()
        self.assertTrue(wait_until(lambda: self.server._heldCounts == {}))
        self.assertEqual(len(self.server._pathsLUT), 0)

    def test_count_goes_back_to_zero_after_an_eviction(self):
        """Counts no locks for a client whose locks the server took back after the maximum lock time."""
        self.server.set_maximum_lock_time(1)
        self.request('x')
        self.assertEqual(self.granted(), 'x')
        self.assertTrue(wait_until(lambda: self.server._heldCounts == {}, seconds=8))

    def test_server_and_other_clients_are_not_limited_by_it(self):
        """Lets the server and a normal client hold as many paths as they need."""
        for index in range(5):
            self.request(f'f{index}')
            self.granted()
        client = self.make_client(self.server.serverFile)
        paths = [self.lock_path(f'other{index}') for index in range(8)]
        self.assertTrue(all(client.acquire_lock(path, timeout=2)[0] for path in paths[:4]))
        self.assertTrue(all(self.server.acquire_lock(path, timeout=2)[0] for path in paths[4:]))

    def test_repeated_path_in_one_request_counts_once(self):
        """Counts a path that is named twice in one request as one held lock, so nothing is lost over time."""
        for cycle in range(12):
            self.request(f'dup{cycle}', ['cap/same', 'cap/same'])
            self.assertEqual(self.granted(), f'dup{cycle}', f'stuck at cycle {cycle}')
            self.assertEqual(sum(self.server._heldCounts.values()), 1)
            self.raw.send({'action': 'release', 'request_unique_id': f'dup{cycle}'})
            self.assertTrue(wait_until(lambda: self.server._heldCounts == {}))

    def test_repeated_paths_do_not_use_up_the_budget_early(self):
        """Grants a request whose distinct paths fit the limit although its list is longer than the limit."""
        self.request('wide-but-few', ['cap/a', 'cap/b'] * 4)
        self.assertEqual(self.granted(), 'wide-but-few')
        self.assertEqual(sum(self.server._heldCounts.values()), 2)

    def test_bad_settings_are_refused(self):
        """Raises ValueError for limits that are not whole numbers of at least 1."""
        for kwargs in ({'maxLocksPerClient': 0}, {'maxPublications': 'x'}, {'maxLoginsInProgress': True}):
            with self.subTest(kwargs):
                with self.assertRaises(ValueError):
                    ServerLocker(PASSWORD, serverFile=False, autoconnect=False, **kwargs)


class TestPublicationLimit(LockerTestCase):
    """Checks the limit on stored publications, and that they expire without timers."""

    def setUp(self):
        """Creates a server that stores three publications, and a hand-made client."""
        super().setUp()
        self.logger = logging.getLogger('pylocker.test.publimit')
        self.server = self.make_locker(self.server_file(), maxPublications=3, logger=self.logger)
        self.raw = RawClient(self.server._serverPort)
        self.addCleanup(self.raw.close)

    def publish(self, word, **changes):
        """Sends a publication to everybody.

        :Parameters:
            #. word (str): The message.
            #. changes (dict): Fields of the message to change.
        """
        self.raw.send(publication(message=word, publication_id=word, **changes))

    def stored(self):
        """Lists the stored messages.

        :Returns:
            #. words (list): The stored message keys, sorted.
        """
        return sorted(self.server.messages)

    def test_publication_over_the_limit_is_dropped_with_a_warning(self):
        """Keeps the first three publications and drops the rest."""
        with self.assertLogs(self.logger, level='WARNING') as captured:
            for index in range(6):
                self.publish(f'word{index}')
            self.assertTrue(wait_until(lambda: len(self.stored()) == 3))
            time.sleep(0.4)
        self.assertEqual(self.stored(), ['word0', 'word1', 'word2'])
        self.assertTrue(any('already stored' in line for line in captured.output))

    def test_replacing_a_message_needs_no_room(self):
        """Replaces a message at the limit."""
        for index in range(3):
            self.publish(f'word{index}')
        self.assertTrue(wait_until(lambda: len(self.stored()) == 3))
        self.publish('word1', replace=True, timeout=30)
        self.assertTrue(wait_until(lambda: self.server.get_message('word1').get('timeout') == 30))

    def test_entries_added_to_one_message_count_each(self):
        """Counts every entry that is added to the same message."""
        for index in range(5):
            self.raw.send(publication(message='same', publication_id=f'p{index}', replace=False))
        self.assertTrue(wait_until(lambda: self.server.get_message('same') is not None))
        time.sleep(0.4)
        self.assertEqual(len(self.server._publications['same']), 3)

    def test_removing_a_message_gives_its_room_back(self):
        """Accepts a new publication after one was removed or popped."""
        for index in range(3):
            self.publish(f'word{index}')
        self.assertTrue(wait_until(lambda: len(self.stored()) == 3))
        self.server.pop_message('word0')
        self.publish('word3')
        self.assertTrue(wait_until(lambda: self.server.has_message('word3')))
        self.server.remove_published_message('word1')
        self.publish('word4')
        self.assertTrue(wait_until(lambda: self.server.has_message('word4')))

    def test_expired_publications_are_gone_and_free_their_room(self):
        """Forgets a publication after its timeout, whether it is looked up or room is needed."""
        for index in range(3):
            self.publish(f'short{index}', timeout=1)
        self.assertTrue(wait_until(lambda: len(self.stored()) == 3))
        time.sleep(1.3)
        # Nothing is looked up here, so the room can only come from forgetting the expired entries
        self.publish('fresh')
        self.assertTrue(wait_until(lambda: self.server.has_message('fresh')))
        self.assertEqual(self.stored(), ['fresh'])
        self.assertFalse(self.server.has_message('short0'))

    def test_expiry_needs_no_background_timer(self):
        """Expires a publication of a locker that is not started."""
        locker = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False))
        locker._store_publication({'message': 'idle', 'timeout': 0.3, 'publication_id': 'i'})
        self.assertTrue(locker.has_message('idle'))
        time.sleep(0.5)
        self.assertFalse(locker.has_message('idle'))
        self.assertEqual(locker.messages, [])

    def test_publication_without_a_timeout_stays(self):
        """Keeps a publication that has no timeout."""
        self.publish('forever')
        self.assertTrue(wait_until(lambda: self.server.has_message('forever')))
        time.sleep(1.2)
        self.assertTrue(self.server.has_message('forever'))


class TestLoginsInProgress(LockerTestCase):
    """Checks that connections that never log in cannot keep real clients out."""

    def silent_connections(self, port, count):
        """Opens connections that say nothing.

        :Parameters:
            #. port (int): The port of the server.
            #. count (int): How many connections to open.

        :Returns:
            #. connections (list): The open sockets.
        """
        connections = [socket.create_connection(('127.0.0.1', port)) for _ in range(count)]
        for connection in connections:
            self.addCleanup(connection.close)
        return connections

    def test_oldest_silent_connections_are_closed_to_make_room(self):
        """Closes the oldest silent connections and lets a real client log in."""
        server = self.make_locker(self.server_file(), maxLoginsInProgress=3)
        silent = self.silent_connections(server._serverPort, 10)
        self.assertTrue(wait_until(lambda: len(server._loginWriters) <= 3))
        real = RawClient(server._serverPort, uniqueName='real')
        self.addCleanup(real.close)
        self.assertEqual(real.reply['action'], 'welcome')
        silent[0].settimeout(3)
        self.assertEqual(silent[0].recv(10), b'')

    def test_silent_connections_do_not_use_up_the_client_slots(self):
        """Accepts real clients although more silent connections than clients are open."""
        server = self.make_locker(self.server_file(), maxClients=2, maxLoginsInProgress=50)
        self.silent_connections(server._serverPort, 10)
        first = RawClient(server._serverPort, uniqueName='first')
        second = RawClient(server._serverPort, uniqueName='second')
        for client in (first, second):
            self.addCleanup(client.close)
            self.assertEqual(client.reply['action'], 'welcome')

    def test_pool_is_empty_when_logins_are_done(self):
        """Forgets a connection once its login has ended, whether it worked or not."""
        server = self.make_locker(self.server_file())
        good = RawClient(server._serverPort, uniqueName='good')
        bad = RawClient(server._serverPort, uniqueName='bad', password='WRONG')
        for client in (good, bad):
            self.addCleanup(client.close)
        self.assertEqual(bad.reply['reason'], 'bad password')
        self.assertTrue(wait_until(lambda: len(server._loginWriters) == 0))


class TestErrorsAreLogged(LockerTestCase):
    """Checks that errors that used to be hidden are now in the log."""

    def setUp(self):
        """Creates a logger that the tests listen to."""
        super().setUp()
        self.logger = logging.getLogger('pylocker.test.errorlog')

    def test_unreadable_message_from_the_server_is_logged(self):
        """Logs a warning for a line the client cannot read, and goes on reading."""
        fake = FakeServer()
        self.addCleanup(fake.close)
        client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                         connectTimeout=3, logger=self.logger))
        client.start(address='127.0.0.1', port=fake.port, connectAttempts=1)
        with self.assertLogs(self.logger, level='WARNING') as captured:
            fake.send_raw(b'this is not json\n')
            time.sleep(0.4)
        self.assertTrue(any('could not be read' in line for line in captured.output))
        self.assertTrue(client.isClient)

    def test_failure_to_clear_the_server_file_is_logged(self):
        """Logs a warning when the server file cannot be emptied."""
        server = self.make_locker(self.server_file(), logger=self.logger)
        with mock.patch.object(lockerModule, '_write_server_file_text', side_effect=OSError('disk full')):
            with self.assertLogs(self.logger, level='WARNING') as captured:
                server._clear_fingerprint()
        self.assertTrue(any('Unable to clear the server file' in line and 'disk full' in line
                            for line in captured.output))

    def test_failure_to_create_the_server_file_is_logged(self):
        """Logs a warning when the server file cannot be created, for a reason other than permission."""
        locker = ServerLocker(PASSWORD, serverFile=self.server_file(), autoconnect=False, logger=self.logger)
        self.track(locker)
        with mock.patch.object(lockerModule, '_write_server_file_text', side_effect=OSError('no space')):
            with self.assertLogs(self.logger, level='WARNING') as captured:
                locker._prepare_server_file()
        self.assertTrue(any('Unable to create the server file' in line and 'no space' in line
                            for line in captured.output))

    def test_connect_attempts_replaces_the_old_name(self):
        """Refuses the old argument name and accepts the new one."""
        locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False)
        with self.assertRaises(TypeError):
            locker.start(address='127.0.0.1', port=9, ntrials=1)
        self.assertFalse(hasattr(ServerLocker, '_clientsQueue'))


if __name__ == '__main__':
    unittest.main()

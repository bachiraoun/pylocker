"""Tests the properties and small methods of a locker: state views, logging, reset, messages, paths and error codes."""
import logging
import os
import unittest

from pylocker import Locker as lockerModule
from pylocker import ServerLocker

from helpers import LockerTestCase, PASSWORD, run_child, wait_until


def idle_locker(**kwargs):
    """Builds a locker that does not start.

    :Parameters:
        #. kwargs (dict): Arguments of ``ServerLocker``, replacing the defaults.

    :Returns:
        #. locker (ServerLocker): The idle locker.
    """
    arguments = {'password': PASSWORD, 'autoconnect': False, 'serverFile': False}
    arguments.update(kwargs)
    return ServerLocker(**arguments)


class TestIdentityProperties(LockerTestCase):
    """Checks the properties that describe a locker before it connects."""

    def test_name_defaults_to_the_unique_name(self):
        """Names a locker after its unique name unless a name is given."""
        locker = idle_locker()
        self.assertEqual(locker.name, locker.uniqueName)
        self.assertEqual(idle_locker(name='mine').name, 'mine')

    def test_unique_names_differ(self):
        """Gives every locker its own unique name."""
        self.assertNotEqual(idle_locker().uniqueName, idle_locker().uniqueName)

    def test_settings_are_readable(self):
        """Returns the settings given to the constructor."""
        locker = idle_locker(defaultTimeout=7, maxLockTime=70, port=4321, debugMode=True)
        self.assertEqual((locker.defaultTimeout, locker.maxLockTime, locker.port), (7, 70, 4321))
        self.assertTrue(locker.debugMode)
        self.assertEqual(locker.pid, os.getpid())
        self.assertIsNone(locker.serverFile)

    def test_password_is_bytes(self):
        """Returns the password as bytes, also after it was changed."""
        locker = idle_locker()
        self.assertEqual(locker.password, b'pw')
        locker.set_password('other')
        self.assertEqual(locker.password, b'other')

    def test_fingerprint_template_has_placeholders(self):
        """Gives a fingerprint line with the time and the port still open."""
        locker = idle_locker()
        self.assertIn('{now}', locker.fingerprint)
        self.assertIn('{port}', locker.fingerprint)
        self.assertIn(locker.uniqueName, locker.fingerprint)
        self.assertIn(f'[{os.getpid()}]', locker.fingerprint)

    def test_remote_orders_property_hides_the_password(self):
        """Reports whether orders are allowed and never the orders password."""
        locker = idle_locker()
        self.assertEqual(locker.allowRemoteOrders, {'allow': False})
        locker.allow_receiving_remote_orders(allow=True, password='secret')
        self.assertEqual(locker.allowRemoteOrders, {'allow': True})

    def test_can_serve(self):
        """Allows serving only with allowServing and a server file."""
        self.assertFalse(idle_locker().canServe)
        self.assertTrue(idle_locker(serverFile=self.server_file()).canServe)
        self.assertFalse(idle_locker(serverFile=self.server_file(), allowServing=False).canServe)


class TestConnectionProperties(LockerTestCase):
    """Checks what a connected locker knows about its server."""

    def setUp(self):
        """Creates a server and a client."""
        super().setUp()
        self.server, self.client = self.make_pair(name='theServer', maxLockTime=90)

    def test_client_knows_the_server(self):
        """Gives the name, unique name, address, port and maximum lock time of the server."""
        self.assertEqual(self.client.serverName, 'theServer')
        self.assertEqual(self.client.serverUniqueName, self.server.uniqueName)
        address, port = self.server_address(self.server)
        self.assertEqual((self.client.serverAddress, self.client.serverPort), (address, port))
        self.assertEqual(self.client.serverMaxLockTime, 90)

    def test_server_has_no_server_details(self):
        """Leaves the server details empty on the server itself."""
        self.assertIsNone(self.server.serverName)
        self.assertIsNone(self.server.serverPort)

    def test_state_flags(self):
        """Tells a server from a client, and a client cannot serve."""
        self.assertTrue(self.server.isServer and not self.server.isClient)
        self.assertTrue(self.client.isClient and not self.client.isServer)
        self.assertTrue(self.server.canServe)
        self.assertFalse(self.client.canServe)


class TestLockViews(LockerTestCase):
    """Checks the views of held locks."""

    def setUp(self):
        """Creates a server and a client."""
        super().setUp()
        self.server, self.client = self.make_pair()

    def test_owned_locks(self):
        """Lists the locks a locker holds, with their paths, until they are released."""
        first, second = self.lock_path('p1'), self.lock_path('p2')
        self.assertEqual(self.client.ownedLocks, {})
        acquired, lockId = self.client.acquire_lock([first, second], timeout=2)
        self.assertTrue(acquired)
        self.assertEqual(self.client.ownedLocks, {lockId: [first, second]})
        self.client.release_lock(lockId)
        self.assertEqual(self.client.ownedLocks, {})

    def test_locked_paths_on_the_server(self):
        """Shows the server every locked path, and shows a client nothing."""
        first, second = self.lock_path('p1'), self.lock_path('p2')
        self.client.acquire_lock([first, second], timeout=2)
        self.assertEqual(sorted(self.server.lockedPaths), sorted([first, second]))
        self.assertIsNone(self.client.lockedPaths)

    def test_client_locks_group_paths_by_lock(self):
        """Groups the paths of one request under its lock id on the server."""
        first, second = self.lock_path('p1'), self.lock_path('p2')
        acquired, lockId = self.client.acquire_lock([first, second], timeout=2)
        self.assertTrue(acquired)
        grouped = self.server.clientLocks
        self.assertEqual(list(grouped), [lockId])
        self.assertEqual(sorted(grouped[lockId]['paths']), sorted([first, second]))
        self.assertIsNone(self.client.clientLocks)

    def test_released_paths_leave_the_views(self):
        """Removes released locks from the server views."""
        acquired, lockId = self.client.acquire_lock(self.lock_path('gone'), timeout=2)
        self.assertTrue(acquired)
        self.client.release_lock(lockId)
        self.assertTrue(wait_until(lambda: self.server.lockedPaths == {}))
        self.assertEqual(self.server.clientLocks, {})

    def test_server_own_locks_are_listed(self):
        """Lists the locks the server holds itself."""
        acquired, lockId = self.server.acquire_lock(self.lock_path('mine'), timeout=2)
        self.assertTrue(acquired)
        self.assertIn(lockId, self.server.ownedLocks)
        self.assertEqual(len(self.server.lockedPaths), 1)


class TestLogger(LockerTestCase):
    """Checks the logger settings."""

    def test_false_and_none_turn_logging_off(self):
        """Has no logger when logging is turned off."""
        locker = idle_locker()
        for value in (False, None):
            with self.subTest(value=value):
                locker.set_logger(value)
                self.assertIsNone(locker._logger)

    def test_true_makes_a_stderr_logger(self):
        """Creates the default pylocker logger when asked for True."""
        locker = idle_locker()
        locker.set_logger(True)
        self.assertEqual(locker._logger.name, 'pylocker.ServerLocker')
        locker.set_logger(False)

    def test_a_given_logger_is_used(self):
        """Sends warnings to the logger that was passed."""
        logger = logging.getLogger('pylocker.test.properties')
        locker = idle_locker(logger=logger)
        with self.assertLogs(logger, level='WARNING') as captured:
            locker._warn('hello from the test')
        self.assertIn('hello from the test', captured.output[0])

    def test_debug_mode_sets_the_level(self):
        """Switches the logger level with debugMode."""
        logger = logging.getLogger('pylocker.test.level')
        locker = idle_locker(logger=logger)
        locker.debugMode = True
        self.assertEqual(logger.level, logging.DEBUG)
        locker.debugMode = False
        self.assertEqual(logger.level, logging.WARNING)


class TestServerFileSetting(unittest.TestCase):
    """Checks set_server_file."""

    def test_forms(self):
        """Accepts a path, True for the default file in the home folder, and False for none."""
        locker = idle_locker()
        locker.set_server_file('/some/where.lock')
        self.assertEqual(locker.serverFile, '/some/where.lock')
        locker.set_server_file(True)
        self.assertEqual(locker.serverFile, os.path.join(os.path.expanduser('~'), '.pylocker.serverlocker'))
        locker.set_server_file(False)
        self.assertIsNone(locker.serverFile)


class TestReset(LockerTestCase):
    """Checks reset."""

    def test_running_locker_is_not_reset(self):
        """Refuses to reset a running locker, and raises when asked to."""
        server = self.make_locker(self.server_file())
        success, reason = server.reset()
        self.assertFalse(success)
        self.assertIn('stop()', reason)
        with self.assertRaises(RuntimeError):
            server.reset(raiseError=True)

    def test_stopped_locker_is_reset(self):
        """Clears the held locks and the messages of a stopped locker."""
        server, client = self.make_pair()
        client.acquire_lock(self.lock_path('x'), timeout=2)
        client.publish_message('note', toSelf=True)
        self.assertTrue(wait_until(lambda: client.has_message('note')))
        client.stop()
        self.assertEqual(client.reset(), (True, None))
        self.assertEqual(client.ownedLocks, {})
        self.assertEqual(client.messages, [])

    def test_reset_lets_a_former_client_serve_again(self):
        """Lets a locker that has been a client become a server again after a reset."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        client = self.make_locker(serverFile)
        self.assertTrue(client.isClient)
        self.assertFalse(client.canServe)
        client.stop()
        server.stop()
        client.reset()
        self.assertTrue(client.canServe)
        client.start()
        self.assertTrue(client.isServer)


class TestMessageStore(LockerTestCase):
    """Checks how stored messages are read and removed."""

    def setUp(self):
        """Creates a server and two clients."""
        super().setUp()
        serverFile = self.server_file()
        self.server = self.make_locker(serverFile)
        self.first = self.make_client(serverFile)
        self.second = self.make_client(serverFile)

    def publish_and_wait(self, publisher, message, **kwargs):
        """Publishes a message and waits until the second client holds it.

        :Parameters:
            #. publisher (ServerLocker): The locker that publishes.
            #. message (str): The message key.
            #. kwargs (dict): More arguments for ``publish_message``.
        """
        publisher.publish_message(message, **kwargs)
        self.assertTrue(wait_until(lambda: self.second.has_message(message)))

    def test_get_message(self):
        """Returns the stored message without removing it, or None."""
        self.assertIsNone(self.second.get_message('absent'))
        self.publish_and_wait(self.first, 'note')
        stored = self.second.get_message('note')
        self.assertEqual(stored['message'], 'note')
        self.assertTrue(self.second.has_message('note'))

    def test_messages_lists_the_keys(self):
        """Lists the keys of the stored messages."""
        self.publish_and_wait(self.first, 'one')
        self.publish_and_wait(self.first, 'two')
        self.assertEqual(sorted(self.second.messages), ['one', 'two'])

    def test_remove_published_message(self):
        """Removes every copy of a message, and the alias does the same."""
        self.publish_and_wait(self.first, 'gone')
        self.assertTrue(self.second.remove_published_message('gone'))
        self.assertFalse(self.second.has_message('gone'))
        self.publish_and_wait(self.first, 'gone2')
        self.assertTrue(self.second.remove_message('gone2'))
        self.assertFalse(self.second.has_message('gone2'))

    def test_remove_by_sender_keeps_other_senders(self):
        """Removes only the entries of the listed senders, keeping the others."""
        self.publish_and_wait(self.first, 'shared', replace=False)
        self.second.publish_message('shared', replace=False, toSelf=True)
        self.assertTrue(wait_until(lambda: len(self.server._publications.get('shared', [])) == 2))
        self.assertTrue(wait_until(lambda: len(self.second._publications.get('shared', [])) == 2))
        self.second.remove_published_message('shared', senders=[self.first.uniqueName])
        remaining = self.second._publications['shared']
        self.assertEqual([entry['publisher_unique_name'] for entry in remaining], [self.second.uniqueName])

    def test_remove_absent_message_succeeds(self):
        """Reports success when the message is not stored."""
        self.assertTrue(self.second.remove_published_message('never-published'))
        self.assertTrue(self.second.remove_published_message('never-published', senders=['nobody']))

    def test_remove_by_sender_reports_what_is_left(self):
        """Returns False while entries of other senders remain, and True once none are left."""
        self.publish_and_wait(self.first, 'mixed', replace=False)
        self.assertFalse(self.second.remove_published_message('mixed', senders=['someone-else']))
        self.assertTrue(self.second.has_message('mixed'))
        self.assertTrue(self.second.remove_published_message('mixed', senders=[self.first.uniqueName]))
        self.assertFalse(self.second.has_message('mixed'))

    def test_pop_returns_and_removes(self):
        """Returns the stored message and removes it."""
        self.publish_and_wait(self.first, 'popme')
        self.assertEqual(self.second.pop_message('popme')['message'], 'popme')
        self.assertFalse(self.second.has_message('popme'))
        self.assertIsNone(self.second.pop_message('popme'))


class TestPathsAndCodes(LockerTestCase):
    """Checks the path normaliser and the error codes."""

    def test_normalize_path(self):
        """Gives equal paths one spelling."""
        cases = {'a//b': 'a/b', 'a/./b': 'a/b', 'a/b/': 'a/b', 'a/../b': 'b', 'a\\b': 'a/b', '/x//y/': '/x/y',
                 '//x/y': '/x/y', '//x//y/': '/x/y', '///x': '/x'}
        for given, expected in cases.items():
            with self.subTest(given):
                self.assertEqual(lockerModule._normalize_path(given), expected)

    def test_different_spellings_lock_the_same_path(self):
        """Refuses a second lock on a path written another way."""
        server, client = self.make_pair()
        path = self.lock_path('spell')
        self.assertTrue(server.acquire_lock(path, timeout=2)[0])
        for spelling in (path + '/', path.replace('spell', 'x/../spell'), path.replace('/spell', '//spell'), '/' + path):
            with self.subTest(spelling):
                self.assertEqual(client.acquire_lock(spelling, timeout=0.2, retries=0), (False, 0))

    def test_to_error_code(self):
        """Turns an error whose message is a number into that number, and any other into its text."""
        self.assertEqual(lockerModule._to_error_code(Exception('2')), 2)
        self.assertEqual(lockerModule._to_error_code(Exception('some text')), 'some text')
        self.assertEqual(lockerModule._to_error_code(ConnectionError('Server connection lost')), 1)
        self.assertEqual(lockerModule._to_error_code(ConnectionResetError()), 1)

    def test_error_codes_of_a_locker_that_is_not_running(self):
        """Returns code 2 for acquire, and 0 for a lock that stop() already released or that was never held."""
        server = self.make_locker(self.server_file())
        acquired, lockId = server.acquire_lock(self.lock_path('held'), timeout=2)
        self.assertTrue(acquired)
        server.stop()
        self.assertEqual(server.acquire_lock(self.lock_path('x'), timeout=1), (False, 2))
        self.assertEqual(server.release_lock(lockId), (True, 0))
        self.assertEqual(server.release_lock('never-held'), (True, 0))


class TestExitCleanup(LockerTestCase):
    """Checks what a process leaves behind when it ends normally without stopping its locker."""

    def test_server_file_is_cleared_at_exit(self):
        """Empties the server file when a serving process exits."""
        serverFile = self.server_file()
        completed = run_child(f"locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)\n"
                              "print(locker.isServer)")
        self.assertEqual(completed.stdout.strip(), 'True', completed.stderr[-300:])
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read().strip(), '')

    def test_locks_of_an_exited_client_are_freed(self):
        """Frees the locks of a client process that exits without releasing them."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        path = self.lock_path('exit')
        completed = run_child(f"locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5, "
                              f"allowServing=False)\nprint(locker.acquire_lock({path!r}, timeout=3)[0])")
        self.assertEqual(completed.stdout.strip(), 'True', completed.stderr[-300:])
        self.assertTrue(server.acquire_lock(path, timeout=5, retries=0)[0])

    def test_exit_hook_stops_the_locker(self):
        """Stops the locker and clears the file when the exit hook runs."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        server._on_atexit()
        self.assertFalse(server.isServer)
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read().strip(), '')


if __name__ == '__main__':
    unittest.main()

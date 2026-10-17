"""Tests that a locker refuses bad arguments with a clear error, and accepts good ones."""
import asyncio
import json
import logging
import unittest

from pylocker import ServerLocker

from helpers import LockerTestCase


def build_locker(**kwargs):
    """Builds a locker that does not start, so no server or file is needed.

    :Parameters:
        #. kwargs (dict): Arguments of ``ServerLocker``, replacing the defaults.

    :Returns:
        #. locker (ServerLocker): The idle locker.
    """
    arguments = {'password': 'pw', 'autoconnect': False, 'serverFile': False}
    arguments.update(kwargs)
    return ServerLocker(**arguments)


class TestConstructorArguments(unittest.TestCase):
    """Checks the arguments of the constructor."""

    def test_wrong_type_raises_type_error(self):
        """Raises TypeError for an argument of the wrong type."""
        cases = [('name', {'name': 5}), ('password', {'password': 5}), ('blocking', {'blocking': 1}),
                 ('allowRemoteClients', {'allowRemoteClients': 'x'}), ('sharedLoop', {'sharedLoop': 'x'}),
                 ('serverFile number', {'serverFile': 5}), ('serverFile empty', {'serverFile': ''})]
        for label, kwargs in cases:
            with self.subTest(label):
                with self.assertRaises(TypeError):
                    build_locker(**kwargs)

    def test_wrong_value_raises_value_error(self):
        """Raises ValueError for an argument of the right type and a wrong value."""
        cases = [('name with colon', {'name': 'a:b'}), ('serializer', {'serializer': 'xml'}),
                 ('defaultTimeout nan', {'defaultTimeout': float('nan')}), ('port zero', {'port': 0}),
                 ('port above the range', {'port': 70000}), ('port is a boolean', {'port': True}),
                 ('connectTimeout zero', {'connectTimeout': 0}),
                 ('electionTimeout negative', {'electionTimeout': -1}),
                 ('electionTimeout boolean', {'electionTimeout': True}),
                 ('electionTimeout infinite', {'electionTimeout': float('inf')}),
                 ('defaultTimeout boolean', {'defaultTimeout': True}),
                 ('defaultTimeout infinite', {'defaultTimeout': float('inf')}),
                 ('maxLockTime infinite', {'maxLockTime': float('inf')}),
                 ('connectTimeout infinite', {'connectTimeout': float('inf')})]
        for label, kwargs in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    build_locker(**kwargs)

    def test_positive_number_messages(self):
        """Names the argument in the message when a number must be positive."""
        for name in ('defaultTimeout', 'maxLockTime'):
            for value in (0, -1, 'x'):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(ValueError) as context:
                        build_locker(**{name: value})
                    self.assertEqual(str(context.exception), f'{name} must be a positive number')

    def test_logger_handler_is_added_only_once(self):
        """Keeps one handler on the shared logger however many lockers ask for the default logger."""
        sharedLogger = logging.getLogger('pylocker.ServerLocker')
        before = list(sharedLogger.handlers)
        self.addCleanup(lambda: [sharedLogger.removeHandler(handler) for handler in sharedLogger.handlers
                                 if handler not in before])
        for _ in range(3):
            build_locker(logger=True)
        self.assertLessEqual(len(sharedLogger.handlers) - len(before), 1)

    def test_good_floats_are_accepted(self):
        """Accepts fractional timeouts."""
        self.assertIsNotNone(build_locker(defaultTimeout=0.5, maxLockTime=3.5))

    def test_default_is_local_only(self):
        """Serves local connections only unless allowRemoteClients is set."""
        self.assertFalse(build_locker().allowRemoteClients)
        self.assertTrue(build_locker(allowRemoteClients=True).allowRemoteClients)
        self.assertEqual(build_locker().address, '127.0.0.1')


class TestSetters(unittest.TestCase):
    """Checks the methods that change a setting."""

    def test_setters_reject_bad_values(self):
        """Raises the intended error for a bad value."""
        locker = build_locker()
        cases = [('max lock time text', ValueError, lambda: locker.set_maximum_lock_time('abc')),
                 ('max lock time zero', ValueError, lambda: locker.set_maximum_lock_time(0)),
                 ('default timeout negative', ValueError, lambda: locker.set_default_timeout(-3)),
                 ('default timeout text', ValueError, lambda: locker.set_default_timeout('soon')),
                 ('max lock time boolean', ValueError, lambda: locker.set_maximum_lock_time(True)),
                 ('max lock time infinite', ValueError, lambda: locker.set_maximum_lock_time(float('inf'))),
                 ('default timeout infinite', ValueError, lambda: locker.set_default_timeout(float('inf'))),
                 ('default timeout not a number', ValueError, lambda: locker.set_default_timeout(float('nan'))),
                 ('password type', TypeError, lambda: locker.set_password(5)),
                 ('server file type', TypeError, lambda: locker.set_server_file(5)),
                 ('orders flag type', TypeError, lambda: locker.allow_receiving_remote_orders('yes')),
                 ('debug mode type', TypeError, lambda: setattr(locker, 'debugMode', 1)),
                 ('start with address only', ValueError, lambda: locker.start(address='127.0.0.1')),
                 ('publish message type', TypeError, lambda: locker.publish_message(5)),
                 ('publish timeout negative', ValueError, lambda: locker.publish_message('m', timeout=-1)),
                 ('stop_remote password type', TypeError, lambda: locker.stop_remote(5))]
        for label, errorType, call in cases:
            with self.subTest(label):
                with self.assertRaises(errorType):
                    call()


class TestAcquireArguments(unittest.TestCase):
    """Checks the arguments of the acquire methods."""

    def setUp(self):
        """Creates an idle locker; the checks happen before anything is sent."""
        self.locker = build_locker()

    def test_bad_arguments_raise_value_error(self):
        """Raises ValueError for an empty path or a bad timeout, retries or retryDelay."""
        cases = [('empty list', {'path': []}), ('timeout zero', {'path': 'x', 'timeout': 0}),
                 ('timeout negative', {'path': 'x', 'timeout': -1}), ('timeout text', {'path': 'x', 'timeout': '3'}),
                 ('retries negative', {'path': 'x', 'retries': -1}),
                 ('retryDelay negative', {'path': 'x', 'retryDelay': -1})]
        for label, kwargs in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    self.locker.acquire_lock(**kwargs)

    def test_error_messages(self):
        """Says what is wrong in the message."""
        with self.assertRaisesRegex(ValueError, 'path must not be empty'):
            self.locker.acquire_lock([])
        with self.assertRaisesRegex(ValueError, 'timeout must be a positive number'):
            self.locker.acquire_lock('x', timeout=0)
        with self.assertRaisesRegex(ValueError, 'retries must be a non-negative integer'):
            self.locker.acquire_lock('x', retries=-1)

    def test_async_checks_the_same_arguments(self):
        """Raises ValueError from acquire_async for an empty path or a bad timeout."""
        for kwargs in ({'path': []}, {'path': 'x', 'timeout': 0}, {'path': 'x', 'timeout': 'z'}):
            with self.subTest(kwargs):
                with self.assertRaises(ValueError):
                    asyncio.run(self.locker.acquire_async(**kwargs))

    def test_lock_global_is_gone(self):
        """Raises TypeError when the removed lockGlobal argument is given."""
        with self.assertRaises(TypeError):
            self.locker.acquire_lock('x', lockGlobal=True)
        with self.assertRaises(TypeError):
            asyncio.run(self.locker.acquire_async('x', lockGlobal=True))

    def test_not_started_locker_returns_error_code(self):
        """Returns (False, 2) for a locker that is not started."""
        self.assertEqual(self.locker.acquire_lock('x', timeout=1), (False, 2))


class TestSaveAndLoad(LockerTestCase):
    """Checks that a saved configuration builds the same locker."""

    def test_round_trip(self):
        """Loads a locker with the settings that were saved."""
        original = build_locker(name='saved', defaultTimeout=7, maxLockTime=70)
        configPath = self.lock_path('config.json')
        original.save(configPath)
        loaded = ServerLocker.load(configPath, 'pw', autoconnect=False)
        self.assertEqual((loaded.name, loaded.defaultTimeout, loaded.maxLockTime), ('saved', 7, 70))

    def test_file_with_unknown_settings_is_refused(self):
        """Refuses a file that holds a setting save() never writes, instead of passing it to the constructor."""
        configPath = self.lock_path('config.json')
        with open(configPath, 'w') as fileHandle:
            json.dump({'name': 'x', 'maxClients': 5, 'logger': True}, fileHandle)
        with self.assertRaisesRegex(ValueError, 'logger, maxClients'):
            ServerLocker.load(configPath, 'pw', autoconnect=False)

    def test_file_that_is_not_a_dictionary_is_refused(self):
        """Refuses a file that holds something else than the settings of a locker."""
        configPath = self.lock_path('config.json')
        with open(configPath, 'w') as fileHandle:
            json.dump(['name'], fileHandle)
        with self.assertRaises(ValueError):
            ServerLocker.load(configPath, 'pw', autoconnect=False)


if __name__ == '__main__':
    unittest.main()

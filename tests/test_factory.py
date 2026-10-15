"""Tests ``FACTORY``, which returns one cached locker for each key."""
import threading

from pylocker import FACTORY

from helpers import LockerTestCase


class TestFactory(LockerTestCase):
    """Checks the cache and its password check."""

    def get(self, key, password, **kwargs):
        """Asks the factory for a locker that does not start.

        :Parameters:
            #. key (str): The cache key.
            #. password (str): The locker password.
            #. kwargs (dict): More arguments for the factory, such as ``regenerate``.

        :Returns:
            #. locker (ServerLocker): The cached or new locker.
        """
        return FACTORY(key=key, password=password, serverFile=self.server_file('f.lock'),
                       autoconnect=False, reconnect=False, **kwargs)

    def test_same_key_and_password_give_the_same_locker(self):
        """Returns the cached locker for the same key and password."""
        key = self.lock_path('same')
        self.assertIs(self.get(key, 'alpha'), self.get(key, 'alpha'))

    def test_other_password_raises_without_leaking_it(self):
        """Raises ValueError for a cached key with another password, and names neither password."""
        key = self.lock_path('other')
        self.get(key, 'alpha')
        with self.assertRaises(ValueError) as context:
            self.get(key, 'beta')
        message = str(context.exception)
        self.assertIn('different password', message)
        self.assertNotIn('alpha', message)
        self.assertNotIn('beta', message)

    def test_cache_is_intact_after_the_error(self):
        """Keeps the cached locker after a refused request."""
        key = self.lock_path('intact')
        first = self.get(key, 'alpha')
        with self.assertRaises(ValueError):
            self.get(key, 'beta')
        self.assertIs(self.get(key, 'alpha'), first)

    def test_regenerate_replaces_the_locker_and_password(self):
        """Replaces the locker and the password it is cached with."""
        key = self.lock_path('regen')
        first = self.get(key, 'alpha')
        second = self.get(key, 'beta', regenerate=True)
        self.assertIsNot(first, second)
        self.assertIs(self.get(key, 'beta'), second)
        with self.assertRaises(ValueError):
            self.get(key, 'alpha')

    def test_regenerate_with_the_same_password(self):
        """Builds a new locker when asked to, even with the same password."""
        key = self.lock_path('regen_same')
        first = self.get(key, 'alpha')
        self.assertIsNot(self.get(key, 'alpha', regenerate=True), first)

    def test_keys_are_independent(self):
        """Keeps one locker for each key."""
        self.assertIsNot(self.get(self.lock_path('k1'), 'a'), self.get(self.lock_path('k2'), 'b'))

    def test_text_and_bytes_password_count_as_different(self):
        """Treats the same password as text and as bytes as two passwords."""
        key = self.lock_path('bytes')
        self.get(key, 'alpha')
        with self.assertRaises(ValueError):
            self.get(key, b'alpha')

    def test_threads_get_one_locker(self):
        """Gives forty threads asking for the same key the same locker."""
        key = self.lock_path('threads')
        lockers = []

        def ask():
            """Asks for the locker and keeps it."""
            lockers.append(self.get(key, 'same'))

        threads = [threading.Thread(target=ask) for _ in range(40)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertEqual(len(lockers), 40)
        self.assertEqual(len({id(locker) for locker in lockers}), 1)

    def test_threads_with_mixed_passwords(self):
        """Keeps one locker and refuses the threads that use the other password."""
        key = self.lock_path('mixed')
        lockers, refused = [], []

        def ask(password):
            """Asks for the locker and keeps it, or notes the refusal."""
            try:
                lockers.append(self.get(key, password))
            except ValueError:
                refused.append(password)

        threads = [threading.Thread(target=ask, args=(f'p{index % 2}',)) for index in range(40)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertEqual(len(lockers) + len(refused), 40)
        self.assertGreater(len(refused), 0)
        self.assertEqual(len({id(locker) for locker in lockers}), 1)

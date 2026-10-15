"""Tests ``SingleLocker``, the form of the locker that exists once for each process."""
import unittest

from pylocker import ServerLocker, SingleLocker

from helpers import LockerTestCase, run_child


class TestSingleLocker(LockerTestCase):
    """Checks that there is one instance and that it works like a locker."""

    def test_is_a_server_locker(self):
        """Derives from ServerLocker."""
        self.assertTrue(issubclass(SingleLocker, ServerLocker))

    def test_every_call_returns_the_same_instance(self):
        """Gives back the first instance and ignores the arguments of later calls."""
        first = SingleLocker('pw', serverFile=False, autoconnect=False, name='first')
        second = SingleLocker('other', serverFile=False, autoconnect=False, name='second')
        self.assertIs(first, second)
        self.assertEqual(second.name, 'first')

    def test_works_as_a_locker_in_a_fresh_process(self):
        """Serves, locks, and returns the same instance, in a process that has no other SingleLocker."""
        serverFile = self.server_file()
        code = f"""
        from pylocker import SingleLocker
        first = SingleLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)
        second = SingleLocker('ignored')
        print(first is second, first.isServer, first.acquire_lock({self.lock_path('single')!r}, timeout=2)[0])
        first.stop()
        """
        completed = run_child(code)
        self.assertEqual(completed.stdout.strip().splitlines()[-1], 'True True True', completed.stderr[-300:])

    def test_single_locker_joins_a_server_in_another_process(self):
        """Connects as a client to a server that another locker runs."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        code = f"""
        from pylocker import SingleLocker
        single = SingleLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)
        print(single.isClient, single.acquire_lock({self.lock_path('joined')!r}, timeout=2)[0])
        single.stop()
        """
        completed = run_child(code)
        self.assertEqual(completed.stdout.strip().splitlines()[-1], 'True True', completed.stderr[-300:])
        self.assertTrue(server.isServer)


if __name__ == '__main__':
    unittest.main()

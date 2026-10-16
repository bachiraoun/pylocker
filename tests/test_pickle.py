"""Tests copying a locker through pickle and the copy module, in the same process and in another one."""
import copy
import json
import os
import pickle
import threading
import time
import uuid

from pylocker import Locker as lockerModule
from pylocker import AuthenticationError, ServerLocker

from helpers import LockerTestCase, PASSWORD, run_child


class TestPickledSettings(LockerTestCase):
    """Checks what a copy of an idle locker holds."""

    def setUp(self):
        """Creates an idle locker with settings that are not pickled."""
        super().setUp()
        self.original = ServerLocker(PASSWORD, serverFile=self.server_file(), autoconnect=False,
                                     connectTimeout=7, electionTimeout=3, debugMode=True)

    def test_constructor_default_matches_the_constant(self):
        """Uses the module constant as the default connectTimeout."""
        self.assertEqual(lockerModule._DEFAULT_CONNECT_TIMEOUT, 20)
        self.assertEqual(ServerLocker(PASSWORD, serverFile=False, autoconnect=False)._ServerLocker__connectTimeout, 20.0)

    def test_startup_settings_are_restored_for_every_protocol(self):
        """Restores connectTimeout and electionTimeout to their defaults and the process id to the current one."""
        for protocol in range(0, pickle.HIGHEST_PROTOCOL + 1):
            with self.subTest(protocol=protocol):
                state = pickle.loads(pickle.dumps(self.original, protocol=protocol)).__dict__
                self.assertEqual(state['_ServerLocker__connectTimeout'], 20.0)
                self.assertIsInstance(state['_ServerLocker__connectTimeout'], float)
                self.assertIsNone(state['_ServerLocker__electionTimeout'])
                self.assertEqual(state['_ServerLocker__pid'], os.getpid())

    def test_original_keeps_its_settings(self):
        """Leaves the settings of the original untouched."""
        pickle.dumps(self.original)
        self.assertEqual(self.original._ServerLocker__connectTimeout, 7.0)
        self.assertEqual(self.original._ServerLocker__electionTimeout, 3.0)

    def test_copy_module(self):
        """Restores the defaults in a shallow and a deep copy."""
        self.assertIsNone(copy.copy(self.original)._ServerLocker__electionTimeout)
        self.assertEqual(copy.deepcopy(self.original)._ServerLocker__connectTimeout, 20.0)

    def test_double_round_trip(self):
        """Survives being pickled again after it was unpickled."""
        twice = pickle.loads(pickle.dumps(pickle.loads(pickle.dumps(self.original))))
        self.assertEqual(twice._ServerLocker__connectTimeout, 20.0)
        self.assertEqual(twice.name, self.original.name)

    def test_user_settings_are_kept(self):
        """Keeps the settings that are pickled."""
        clone = pickle.loads(pickle.dumps(self.original))
        for name in ('defaultTimeout', 'maxLockTime', 'serverFile', 'debugMode', 'name'):
            with self.subTest(name):
                self.assertEqual(getattr(clone, name), getattr(self.original, name))

    def test_password_is_not_pickled(self):
        """Leaves the password out of the pickle."""
        self.assertIsNone(pickle.loads(pickle.dumps(self.original)).password)

    def test_copy_is_idle(self):
        """Gives a copy that is neither server nor client."""
        clone = pickle.loads(pickle.dumps(self.original))
        self.assertIsNone(clone._loop)
        self.assertEqual(clone._bgTasks, [])
        self.assertFalse(clone.isServer)
        self.assertFalse(clone.isClient)


class TestStartingACopy(LockerTestCase):
    """Checks that a copy can start, once it has the password and an identity of its own."""

    @staticmethod
    def revive(blob):
        """Turns pickled bytes back into a locker that can start.

        :Parameters:
            #. blob (bytes): The pickled locker.

        :Returns:
            #. locker (ServerLocker): The copy, with the password set and a new unique name.
        """
        clone = pickle.loads(blob)
        clone._ServerLocker__password = lockerModule._to_bytes(PASSWORD)
        clone._ServerLocker__uniqueName = str(uuid.uuid4())
        return clone

    def test_copy_starts_as_a_client_in_the_same_process(self):
        """Connects a copy to the running server and lets it lock."""
        server = self.make_locker(self.server_file())
        clone = self.track(self.revive(pickle.dumps(server)))
        clone.start()
        self.assertTrue(clone.isClient)
        path = self.lock_path('k1')
        acquired, _ = clone.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.assertEqual(server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        clone.stop()
        self.assertTrue(server.acquire_lock(path, timeout=3)[0])

    def test_copy_restarts_after_stop(self):
        """Starts again after it was stopped."""
        server = self.make_locker(self.server_file())
        clone = self.track(self.revive(pickle.dumps(server)))
        clone.start()
        clone.stop()
        clone.start()
        self.assertTrue(clone.isClient)

    def test_many_copies_start_together(self):
        """Starts twelve copies at once as clients, each locking its own path."""
        server = self.make_locker(self.server_file())
        blob = pickle.dumps(server)
        clones = [self.track(self.revive(blob)) for _ in range(12)]
        errors = []

        def start(clone):
            """Starts one copy and keeps any error."""
            try:
                clone.start()
            except BaseException as error:
                errors.append(repr(error))

        threads = [threading.Thread(target=start, args=(clone,)) for clone in clones]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertEqual(errors, [])
        self.assertTrue(all(clone.isClient for clone in clones))
        self.assertTrue(all(clone.acquire_lock(self.lock_path(f'c{index}'), timeout=3)[0]
                            for index, clone in enumerate(clones)))
        self.assertEqual(clones[0].acquire_lock(self.lock_path('c1'), timeout=0.3, retries=0), (False, 0))

    def test_start_without_a_password_is_refused(self):
        """Raises the authentication error, not a crash, when the copy has no password."""
        server = self.make_locker(self.server_file())
        clone = pickle.loads(pickle.dumps(server))
        clone._ServerLocker__uniqueName = str(uuid.uuid4())
        with self.assertRaises(AuthenticationError):
            clone.start()


class TestCopyInAnotherProcess(LockerTestCase):
    """Checks a copy that is started in a process other than the one that pickled it."""

    def test_copy_of_a_running_server_becomes_a_client(self):
        """Connects to the server of the pickling process and leaves its server file alone."""
        server = self.make_locker(self.server_file())
        blobFile = self.lock_path('server.pkl')
        with open(blobFile, 'wb') as fileHandle:
            fileHandle.write(pickle.dumps(server))
        code = f"""
        import json, pickle, uuid
        from pylocker import Locker as lockerModule
        clone = pickle.load(open({blobFile!r}, 'rb'))
        clone._ServerLocker__password = lockerModule._to_bytes(PASSWORD)
        clone._ServerLocker__uniqueName = str(uuid.uuid4())
        clone.start()
        fingerprint = clone.get_running_server_fingerprint()
        acquired = clone.acquire_lock({self.lock_path('xp1')!r}, timeout=3)[0]
        print(json.dumps({{'isClient': clone.isClient, 'isServer': clone.isServer, 'acquired': acquired,
                           'serverPid': fingerprint[4], 'ownPid': os.getpid()}}))
        clone.stop()
        """
        completed = run_child(code)
        self.assertEqual(completed.returncode, 0, completed.stderr[-400:])
        info = json.loads(completed.stdout.strip().splitlines()[-1])
        self.assertTrue(info['isClient'])
        self.assertFalse(info['isServer'])
        self.assertTrue(info['acquired'])
        self.assertEqual(int(info['serverPid']), os.getpid())
        self.assertNotEqual(info['ownPid'], os.getpid())

    def test_copy_becomes_the_server_with_its_own_process_id(self):
        """Writes the copy's own process id, not the pickling process's, in the server file."""
        serverFile = self.server_file('fresh.lock')
        source = ServerLocker(PASSWORD, serverFile=serverFile, autoconnect=False)
        blobFile = self.lock_path('source.pkl')
        with open(blobFile, 'wb') as fileHandle:
            fileHandle.write(pickle.dumps(source))
        code = f"""
        import json, pickle, uuid
        from pylocker import Locker as lockerModule
        clone = pickle.load(open({blobFile!r}, 'rb'))
        clone._ServerLocker__password = lockerModule._to_bytes(PASSWORD)
        clone._ServerLocker__uniqueName = str(uuid.uuid4())
        clone.start()
        fingerprint = clone.get_running_server_fingerprint()
        acquired = clone.acquire_lock({self.lock_path('xp2')!r}, timeout=3)[0]
        print(json.dumps({{'isServer': clone.isServer, 'acquired': acquired, 'serverPid': fingerprint[4],
                           'ownPid': os.getpid(), 'line': open({serverFile!r}).read().strip()}}))
        clone.stop()
        """
        completed = run_child(code)
        self.assertEqual(completed.returncode, 0, completed.stderr[-400:])
        info = json.loads(completed.stdout.strip().splitlines()[-1])
        self.assertTrue(info['isServer'])
        self.assertTrue(info['acquired'])
        self.assertEqual(int(info['serverPid']), info['ownPid'])
        self.assertNotEqual(info['ownPid'], os.getpid())
        self.assertIn(f"[{info['ownPid']}]", info['line'])

    def test_lock_of_a_copy_that_dies_is_freed(self):
        """Frees the lock of a copy whose process ends without cleaning up."""
        server = self.make_locker(self.server_file())
        blobFile = self.lock_path('s5.pkl')
        with open(blobFile, 'wb') as fileHandle:
            fileHandle.write(pickle.dumps(server))
        code = f"""
        import pickle, uuid
        from pylocker import Locker as lockerModule
        clone = pickle.load(open({blobFile!r}, 'rb'))
        clone._ServerLocker__password = lockerModule._to_bytes(PASSWORD)
        clone._ServerLocker__uniqueName = str(uuid.uuid4())
        clone.start()
        print(clone.acquire_lock({self.lock_path('dead1')!r}, timeout=3)[0], flush=True)
        os._exit(0)
        """
        completed = run_child(code)
        self.assertTrue(completed.stdout.strip().endswith('True'), completed.stderr[-400:])
        start = time.time()
        acquired, _ = server.acquire_lock(self.lock_path('dead1'), timeout=15, retries=0)
        self.assertTrue(acquired)
        self.assertLess(time.time() - start, 12)

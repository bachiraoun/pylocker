"""Tests how lockers choose their server through the server file, including broken and half-written files."""
import logging
import os
import signal
import subprocess
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule
from pylocker import ServerLocker

from helpers import LockerTestCase, PASSWORD, dead_process_id, spawn_child, start_sleeper, wait_until

HAS_FILE_LOCKING = lockerModule.fcntl is not None


class TestElectionFiles(LockerTestCase):
    """Checks what a new locker does with the server file contents it finds."""

    def start_locker(self, serverFile, **kwargs):
        """Starts a locker with a short election timeout and returns it with the seconds it took.

        :Parameters:
            #. serverFile (str): The server file path.
            #. kwargs (dict): More arguments for ``ServerLocker``.

        :Returns:
            #. locker (ServerLocker): The started locker.
            #. seconds (float): How long the start took.
        """
        start = time.time()
        locker = self.make_locker(serverFile, electionTimeout=15, **kwargs)
        return locker, time.time() - start

    def write_file(self, text):
        """Writes the server file and returns its path.

        :Parameters:
            #. text (str): The file content.

        :Returns:
            #. serverFile (str): The server file path.
        """
        serverFile = self.server_file()
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write(text)
        return serverFile

    def test_pending_server_of_a_live_process_is_not_stolen(self):
        """Waits for a server that is still starting, and serves only once its entry is too old."""
        owner = start_sleeper()
        self.addCleanup(owner.kill)
        serverFile = self.write_file(f'starting({time.time()})@127.0.0.1:PORT[{owner.pid}]\n')
        locker, seconds = self.start_locker(serverFile)
        self.assertTrue(locker.isServer)
        self.assertTrue(1.5 <= seconds <= 5, seconds)

    def test_client_only_gives_up_on_a_pending_server(self):
        """Waits for a pending server, then gives up when the client may not serve."""
        owner = start_sleeper()
        self.addCleanup(owner.kill)
        serverFile = self.write_file(f'starting({time.time()})@127.0.0.1:PORT[{owner.pid}]\n')
        locker, seconds = self.start_locker(serverFile, allowServing=False)
        self.assertFalse(locker.isServer)
        self.assertFalse(locker.isClient)
        self.assertTrue(1.5 <= seconds <= 6, seconds)

    def test_stuck_claim_of_a_live_process_is_abandoned(self):
        """Takes over a claim that never turned into a server, once it is about two seconds old."""
        owner = start_sleeper()
        self.addCleanup(owner.kill)
        serverFile = self.write_file(f'ghost(TIMESTAMP)@127.0.0.1:PORT[{owner.pid}]\n')
        locker, seconds = self.start_locker(serverFile)
        self.assertTrue(locker.isServer)
        self.assertTrue(1.5 <= seconds <= 5, seconds)

    def test_claim_of_a_dead_process_is_taken_at_once(self):
        """Takes over a claim whose process is gone without waiting."""
        serverFile = self.write_file(f'ghost(TIMESTAMP)@127.0.0.1:PORT[{dead_process_id()}]\n')
        locker, seconds = self.start_locker(serverFile)
        self.assertTrue(locker.isServer)
        self.assertLess(seconds, 1.0)

    def test_claim_of_another_machine_is_not_probed(self):
        """Does not check a process id from another machine, and takes over when the claim ages out."""
        serverFile = self.write_file('far(TIMESTAMP)@10.9.9.9:PORT[999999]\n')
        locker, seconds = self.start_locker(serverFile)
        self.assertTrue(locker.isServer)
        self.assertTrue(1.5 <= seconds <= 5, seconds)

    def test_election_timeout_raises(self):
        """Raises RuntimeError when the election cannot finish within the election timeout."""
        owner = start_sleeper()
        self.addCleanup(owner.kill)
        serverFile = self.write_file(f'starting({time.time()})@127.0.0.1:PORT[{owner.pid}]\n')
        start = time.time()
        with self.assertRaisesRegex(RuntimeError, 'election timed out'):
            ServerLocker(PASSWORD, serverFile=serverFile, connectTimeout=5, electionTimeout=0.5)
        self.assertLess(time.time() - start, 3)

    def test_garbage_file_does_not_hang(self):
        """Becomes the server when the file holds text that is no fingerprint."""
        locker, seconds = self.start_locker(self.write_file('this is not a fingerprint\n'))
        self.assertTrue(locker.isServer)
        self.assertLess(seconds, 2)

    def test_second_locker_joins_the_first(self):
        """Connects a second locker to the server of the first."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile)
        client = self.make_locker(serverFile)
        self.assertTrue(server.isServer)
        self.assertTrue(client.isClient)

    def test_server_file_names_the_server_process(self):
        """Writes the server's own process id in the server file."""
        server = self.make_locker(self.server_file())
        fingerprint = server.get_running_server_fingerprint(serverFile=server.serverFile)
        self.assertEqual(int(fingerprint[4]), os.getpid())
        self.assertEqual(fingerprint[0], server.uniqueName)


@unittest.skipUnless(HAS_FILE_LOCKING, 'needs the fcntl module')
class TestElectionLock(LockerTestCase):
    """Checks the lock that keeps two lockers from deciding at the same moment."""

    def hold_lock(self, serverFile):
        """Starts a process that holds the election lock, and returns it once the lock is held.

        :Parameters:
            #. serverFile (str): The server file path; the lock is held on the lock file beside it.

        :Returns:
            #. holder (subprocess.Popen): The process holding the lock.
        """
        lockFile = serverFile + lockerModule._LOCK_FILE_SUFFIX
        open(lockFile, 'a').close()
        code = f"""
        import fcntl, time
        fileHandle = open({lockFile!r}, 'a')
        fcntl.flock(fileHandle, fcntl.LOCK_EX)
        print('HELD', flush=True)
        time.sleep(30)
        """
        holder = spawn_child(code)
        self.addCleanup(holder.kill)
        holder.stdout.readline()
        return holder

    def test_busy_lock_warns_and_still_elects(self):
        """Warns and goes on when the lock stays held past the limit."""
        serverFile = self.server_file()
        self.hold_lock(serverFile)
        logger = logging.getLogger('pylocker.test.busy')
        start = time.time()
        with mock.patch.object(lockerModule, '_ELECTION_LOCK_TIMEOUT', 0.6):
            with self.assertLogs(logger, level='WARNING') as captured:
                locker = self.make_locker(serverFile, electionTimeout=15, logger=logger)
        self.assertTrue(locker.isServer)
        self.assertTrue(any('busy for too long' in line for line in captured.output))
        self.assertLess(time.time() - start, 4)

    def test_killed_lock_holder_frees_the_lock(self):
        """Goes on at once when the process holding the lock is killed."""
        serverFile = self.server_file()
        holder = self.hold_lock(serverFile)
        killer = threading.Timer(0.5, lambda: os.kill(holder.pid, signal.SIGKILL))
        killer.start()
        self.addCleanup(killer.cancel)
        start = time.time()
        locker = self.make_locker(serverFile, electionTimeout=15)
        seconds = time.time() - start
        self.assertTrue(locker.isServer)
        self.assertTrue(0.4 <= seconds < 3, seconds)

    def test_election_works_without_file_locking(self):
        """Elects a server and connects a client when no file locking module exists."""
        with mock.patch.object(lockerModule, 'fcntl', None), mock.patch.object(lockerModule, 'msvcrt', None):
            serverFile = self.server_file()
            server = self.make_locker(serverFile)
            client = self.make_client(serverFile)
            self.assertTrue(server.isServer)
            self.assertTrue(client.isClient)


class TestProcessExists(unittest.TestCase):
    """Checks the test that tells whether a process is alive."""

    def test_own_process_exists(self):
        """Reports the running process as alive."""
        self.assertTrue(lockerModule._process_exists(os.getpid()))

    def test_unused_process_id_does_not_exist(self):
        """Reports an unused process id as gone."""
        self.assertFalse(lockerModule._process_exists(999999))

    def test_process_of_another_user_counts_as_alive(self):
        """Reports process 1, which is not ours, as alive."""
        self.assertTrue(lockerModule._process_exists(1))

    def test_without_psutil(self):
        """Gives the same answers when psutil is not installed."""
        with mock.patch.object(lockerModule, 'psutil', None):
            self.assertTrue(lockerModule._process_exists(os.getpid()))
            self.assertFalse(lockerModule._process_exists(999999))
            self.assertTrue(lockerModule._process_exists(1))

    def test_windows_never_signals(self):
        """Reports alive on Windows without sending a signal to the process."""
        with mock.patch.object(lockerModule, 'psutil', None), mock.patch.object(lockerModule.os, 'name', 'nt'):
            self.assertTrue(lockerModule._process_exists(999999))

    def test_garbage_process_id_raises(self):
        """Raises ValueError for a process id that is no number."""
        with self.assertRaises(ValueError):
            lockerModule._process_exists('abc')


class TestSimultaneousStart(LockerTestCase):
    """Checks that processes starting at the same moment end with exactly one server."""

    PROCESSES = 5
    TRIALS = 6

    def run_trial(self, trial):
        """Starts several processes together on one server file and counts the servers.

        :Parameters:
            #. trial (int): The number of the trial, used to name the files.

        :Returns:
            #. servers (int): How many of the processes became the server.
            #. clients (int): How many of the processes became clients.
        """
        serverFile = self.server_file(f'trial{trial}.lock')
        gate = self.lock_path(f'gate{trial}')
        done = self.lock_path(f'done{trial}')
        code = f"""
        while not os.path.exists({gate!r}):
            time.sleep(0.001)
        locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=10)
        role = 'server' if locker.isServer else ('client' if locker.isClient else 'none')
        open({self.lock_path(f'role{trial}_')!r} + str(os.getpid()), 'w').write(role)
        while not os.path.exists({done!r}):
            time.sleep(0.05)
        locker.stop()
        """
        children = [spawn_child(code, stderr=subprocess.DEVNULL) for _ in range(self.PROCESSES)]
        try:
            open(gate, 'w').close()
            prefix = f'role{trial}_'
            isComplete = wait_until(
                lambda: len([name for name in os.listdir(self.tempFolder) if name.startswith(prefix)]) == self.PROCESSES,
                seconds=60)
            self.assertTrue(isComplete, 'a process did not finish its election')
            roles = [open(os.path.join(self.tempFolder, name)).read()
                     for name in os.listdir(self.tempFolder) if name.startswith(prefix)]
        finally:
            open(done, 'w').close()
            for child in children:
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill()
        return roles.count('server'), roles.count('client')

    def test_exactly_one_server(self):
        """Elects exactly one server in every trial, and every other process becomes its client."""
        for trial in range(self.TRIALS):
            with self.subTest(trial=trial):
                servers, clients = self.run_trial(trial)
                self.assertEqual(servers, 1)
                self.assertEqual(clients, self.PROCESSES - 1)


if __name__ == '__main__':
    unittest.main()

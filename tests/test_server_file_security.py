"""Tests that the server file is created private and is refused when another user could have planted or changed it."""
import os
import shutil
import stat
import subprocess
import sys
import time
import tempfile
import unittest
from unittest import mock

from pylocker import Locker as lockerModule
from pylocker import ServerLocker

from helpers import LockerTestCase, PASSWORD, spawn_child

IS_WINDOWS = os.name == 'nt'


def mode_of(path):
    """Returns the permission bits of a file.

    :Parameters:
        #. path (str): Path of the file.

    :Returns:
        #. mode (int): The permission bits, e.g. ``0o600``.
    """
    return stat.S_IMODE(os.stat(path).st_mode)


@unittest.skipIf(IS_WINDOWS, 'file permissions work differently on Windows')
class TestPrivateCreation(LockerTestCase):
    """Checks the permissions of files that the package creates."""

    def test_new_server_file_is_private_even_with_a_permissive_umask(self):
        """Gives a new server file to its owner alone, whatever the umask says."""
        previous = os.umask(0)
        self.addCleanup(os.umask, previous)
        serverFile = self.server_file()
        self.make_locker(serverFile)
        self.assertEqual(mode_of(serverFile), 0o600)

    def test_existing_file_keeps_its_permissions(self):
        """Leaves the permissions of a file that already exists as they are, and uses it."""
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        os.chmod(serverFile, 0o644)
        server = self.make_locker(serverFile)
        self.assertTrue(server.isServer)
        self.assertEqual(mode_of(serverFile), 0o644)

    def test_client_and_server_work_through_a_private_file(self):
        """Connects a client through a file only the owner can read."""
        server, client = self.make_pair()
        self.assertTrue(client.isClient and server.isServer)
        self.assertEqual(mode_of(server.serverFile), 0o600)


@unittest.skipIf(IS_WINDOWS, 'file permissions work differently on Windows')
class TestRefusedFiles(LockerTestCase):
    """Checks the files and folders that a locker refuses to use."""

    def refused(self, serverFile, text):
        """Asserts that a new locker refuses the file, naming the reason.

        :Parameters:
            #. serverFile (str): Path of the server file.
            #. text (str): Part of the message that the error must contain.
        """
        with self.assertRaisesRegex(PermissionError, text):
            self.track(ServerLocker(PASSWORD, serverFile=serverFile, connectTimeout=3))

    def test_world_writable_file_is_refused(self):
        """Refuses a file that every user can write."""
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        os.chmod(serverFile, 0o666)
        self.refused(serverFile, 'can be written by every user')

    def test_group_writable_file_is_refused(self):
        """Refuses a file that the group of the user can write, since the group could redirect the clients."""
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        os.chmod(serverFile, 0o664)
        self.refused(serverFile, 'can be written by its group')

    def test_refused_file_is_left_untouched(self):
        """Does not empty or change a file it refuses."""
        serverFile = self.server_file()
        with open(serverFile, 'w') as fileHandle:
            fileHandle.write('planted content\n')
        os.chmod(serverFile, 0o666)
        self.refused(serverFile, 'every user')
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read(), 'planted content\n')

    def test_file_of_another_user_is_refused(self):
        """Refuses a file that belongs to another user."""
        if os.getuid() == 0:
            self.skipTest('root trusts every file')
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        with mock.patch.object(os, 'geteuid', return_value=os.getuid() + 4321):
            self.refused(serverFile, 'belongs to another user')

    def test_symbolic_link_is_refused(self):
        """Refuses a server file that is a link to another file."""
        target = self.lock_path('target.txt')
        open(target, 'w').close()
        serverFile = self.server_file()
        os.symlink(target, serverFile)
        self.refused(serverFile, 'not a regular file')

    def test_folder_everyone_can_change_is_refused(self):
        """Refuses a file in a folder that every user can change."""
        folder = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, folder)
        os.chmod(folder, 0o777)
        serverFile = os.path.join(folder, 'server.lock')
        self.addCleanup(lambda: os.path.exists(serverFile) and os.remove(serverFile))
        self.refused(serverFile, 'folder that every user can change')

    def test_sticky_shared_folder_is_accepted(self):
        """Accepts a file in a shared folder with the sticky bit, as /tmp is."""
        folder = tempfile.mkdtemp()
        os.chmod(folder, 0o1777)
        serverFile = os.path.join(folder, 'server.lock')
        self.addCleanup(shutil.rmtree, folder)
        self.assertTrue(self.make_locker(serverFile).isServer)


@unittest.skipIf(IS_WINDOWS, 'file permissions work differently on Windows')
class TestElectionLockFile(LockerTestCase):
    """Checks the private file that holds the election lock."""

    def lock_file_of(self, serverFile):
        """Returns the path of the election lock file that belongs to a server file.

        :Parameters:
            #. serverFile (str): The server file path.

        :Returns:
            #. lockFile (str): The lock file path.
        """
        return serverFile + lockerModule._LOCK_FILE_SUFFIX

    def test_lock_file_is_private_even_with_a_permissive_umask(self):
        """Creates the lock file for its owner alone, whatever the umask says."""
        previous = os.umask(0)
        self.addCleanup(os.umask, previous)
        serverFile = self.server_file()
        self.make_locker(serverFile)
        self.assertEqual(mode_of(self.lock_file_of(serverFile)), 0o600)

    def test_reader_of_an_old_server_file_cannot_hold_up_the_election(self):
        """Starts at once when another user, who may only read the server file, holds a lock on it."""
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        os.chmod(serverFile, 0o644)
        code = f"""
        import fcntl, time
        handle = open({serverFile!r})
        fcntl.flock(handle, fcntl.LOCK_EX)
        print('HELD', flush=True)
        time.sleep(30)
        """
        holder = spawn_child(code)
        self.addCleanup(holder.kill)
        holder.stdout.readline()
        start = time.time()
        locker = self.make_locker(serverFile)
        self.assertTrue(locker.isServer)
        self.assertLess(time.time() - start, 2)

    def test_lock_file_that_everyone_can_write_is_refused(self):
        """Refuses to start when the lock file can be written by every user."""
        serverFile = self.server_file()
        lockFile = self.lock_file_of(serverFile)
        open(lockFile, 'w').close()
        os.chmod(lockFile, 0o666)
        with self.assertRaisesRegex(PermissionError, 'every user'):
            self.track(ServerLocker(PASSWORD, serverFile=serverFile, connectTimeout=3))


@unittest.skipIf(IS_WINDOWS, 'file permissions work differently on Windows')
class TestFileHelpers(LockerTestCase):
    """Checks the functions that read and write the server file."""

    def test_reading_a_planted_file_raises(self):
        """Raises PermissionError from the public reader, and gives empty fields when told not to raise."""
        serverFile = self.server_file()
        open(serverFile, 'w').close()
        os.chmod(serverFile, 0o666)
        locker = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False))
        with self.assertRaises(PermissionError):
            locker.get_running_server_fingerprint(serverFile=serverFile)
        self.assertEqual(locker.get_running_server_fingerprint(serverFile=serverFile, raiseError=False),
                         (None, None, None, None, None))

    def test_write_refuses_a_file_that_became_world_writable(self):
        """Refuses to write once a file that was fine has been made writable for every user."""
        serverFile = self.server_file()
        lockerModule._write_server_file_text(serverFile, 'first\n')
        os.chmod(serverFile, 0o666)
        with self.assertRaises(PermissionError):
            lockerModule._write_server_file_text(serverFile, 'second\n')
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read(), 'first\n')

    def test_write_replaces_longer_content(self):
        """Leaves nothing of the old content when the new content is shorter."""
        serverFile = self.server_file()
        lockerModule._write_server_file_text(serverFile, 'a long first line of text\n')
        lockerModule._write_server_file_text(serverFile, 'short\n')
        with open(serverFile) as fileHandle:
            self.assertEqual(fileHandle.read(), 'short\n')

    def test_empty_text_empties_the_file(self):
        """Empties the file when given an empty text."""
        serverFile = self.server_file()
        lockerModule._write_server_file_text(serverFile, 'content\n')
        lockerModule._write_server_file_text(serverFile, '')
        self.assertEqual(os.path.getsize(serverFile), 0)

    def test_write_does_not_follow_a_link_swapped_in_after_the_check(self):
        """Fails to write through a symbolic link and leaves the file it points to unchanged."""
        target = self.lock_path('target.txt')
        with open(target, 'w') as fileHandle:
            fileHandle.write('target content\n')
        serverFile = self.server_file()
        os.symlink(target, serverFile)
        with self.assertRaises(OSError):
            lockerModule._write_server_file_text(serverFile, 'overwritten\n')
        with open(target) as fileHandle:
            self.assertEqual(fileHandle.read(), 'target content\n')

    def test_read_does_not_follow_a_link(self):
        """Fails to read through a symbolic link."""
        target = self.lock_path('target.txt')
        open(target, 'w').close()
        serverFile = self.server_file()
        os.symlink(target, serverFile)
        with self.assertRaises(OSError):
            lockerModule._read_server_file_line(serverFile)

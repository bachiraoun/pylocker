"""Shared helpers of the pylocker tests: temporary folders, lockers that stop by themselves, and child processes."""
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

import pylocker
from pylocker import ServerLocker

PASSWORD = 'pw'

# The folder that holds the pylocker package, so a child process imports the same copy the tests use.
PACKAGE_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(pylocker.__file__)))

CHILD_PREAMBLE = f"""
import os, sys, time
sys.path.insert(0, {PACKAGE_PARENT!r})
from pylocker import ServerLocker
PASSWORD = {PASSWORD!r}
"""


def wait_until(condition, seconds=5.0, poll=0.05):
    """Waits until a condition is true or the time is up.

    :Parameters:
        #. condition (callable): A function without arguments returning True when the wait is over.
        #. seconds (int, float): The longest time to wait.
        #. poll (int, float): Seconds between two checks of the condition.

    :Returns:
        #. isTrue (bool): Whether the condition was true before the time was up.
    """
    deadline = time.time() + seconds
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(poll)
    return condition()


def child_code(code):
    """Returns the source of a child process: the common imports followed by the given code.

    :Parameters:
        #. code (str): The code to run in the child, indented or not.

    :Returns:
        #. source (str): The complete source to give to ``python -c``.
    """
    return CHILD_PREAMBLE + textwrap.dedent(code)


def run_child(code, timeout=60):
    """Runs code in a child process and waits for it to end.

    :Parameters:
        #. code (str): The code to run in the child.
        #. timeout (int, float): Seconds before the wait is given up.

    :Returns:
        #. completed (subprocess.CompletedProcess): The result, with the output as text.
    """
    return subprocess.run([sys.executable, '-c', child_code(code)],
                          capture_output=True, text=True, timeout=timeout)


def spawn_child(code, stderr=None):
    """Starts code in a child process that keeps running, with its output readable.

    :Parameters:
        #. code (str): The code to run in the child.
        #. stderr (None, int): Where the child's error output goes, such as ``subprocess.DEVNULL``.
           None shows it in the terminal.

    :Returns:
        #. process (subprocess.Popen): The running child, with ``stdout`` piped as text.
    """
    return subprocess.Popen([sys.executable, '-u', '-c', child_code(code)],
                            stdout=subprocess.PIPE, stderr=stderr, text=True)


def start_sleeper():
    """Starts a child process that only sleeps, to stand for a live process.

    :Returns:
        #. process (subprocess.Popen): The sleeping child.
    """
    return subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])


def dead_process_id():
    """Returns the process id of a process that has just ended.

    :Returns:
        #. pid (int): A process id that no live process holds.
    """
    process = subprocess.Popen([sys.executable, '-c', 'pass'])
    process.wait()
    return process.pid


class LockerTestCase(unittest.TestCase):
    """
    A test case with a temporary folder and lockers that are stopped when the test ends.

    Every test gets its own folder, so the server files of two tests never meet.
    """

    def setUp(self):
        """Creates the temporary folder of the test and removes it when the test ends."""
        self.tempFolder = tempfile.mkdtemp(prefix='pylocker_test_')
        self.addCleanup(shutil.rmtree, self.tempFolder, True)

    def server_file(self, name='server.lock'):
        """Returns the path of a server file in the temporary folder.

        :Parameters:
            #. name (str): The file name.

        :Returns:
            #. path (str): The path of the server file.
        """
        return os.path.join(self.tempFolder, name)

    def lock_path(self, name):
        """Returns a path to lock, in the temporary folder.

        :Parameters:
            #. name (str): The name of the entry to lock.

        :Returns:
            #. path (str): The path to give to ``acquire_lock``.
        """
        return os.path.join(self.tempFolder, name)

    @staticmethod
    def _stop_quietly(locker):
        """Stops a locker and ignores errors from one that is already stopped."""
        try:
            locker.stop()
        except Exception:
            pass

    def track(self, locker):
        """Makes sure a locker is stopped when the test ends.

        :Parameters:
            #. locker (ServerLocker): The locker to stop at the end.

        :Returns:
            #. locker (ServerLocker): The same locker.
        """
        self.addCleanup(self._stop_quietly, locker)
        return locker

    def make_locker(self, serverFile, **kwargs):
        """Creates a locker that can serve or connect, and stops it when the test ends.

        :Parameters:
            #. serverFile (str, bool): The server file path.
            #. kwargs (dict): More arguments for ``ServerLocker``, replacing the defaults.

        :Returns:
            #. locker (ServerLocker): The started locker.
        """
        arguments = {'password': PASSWORD, 'serverFile': serverFile, 'connectTimeout': 5}
        arguments.update(kwargs)
        return self.track(ServerLocker(**arguments))

    def make_client(self, serverFile, **kwargs):
        """Creates a locker that only connects, and stops it when the test ends.

        :Parameters:
            #. serverFile (str, bool): The server file path.
            #. kwargs (dict): More arguments for ``ServerLocker``, replacing the defaults.

        :Returns:
            #. locker (ServerLocker): The connected locker.
        """
        kwargs.setdefault('allowServing', False)
        return self.make_locker(serverFile, **kwargs)

    def make_pair(self, **kwargs):
        """Creates a server and a client that share a new server file.

        :Parameters:
            #. kwargs (dict): More arguments for the server, replacing the defaults.

        :Returns:
            #. server (ServerLocker): The locker that serves.
            #. client (ServerLocker): The locker connected to it.
        """
        serverFile = self.server_file()
        server = self.make_locker(serverFile, **kwargs)
        client = self.make_client(serverFile)
        self.assertTrue(server.isServer)
        self.assertTrue(client.isClient)
        return server, client

    @staticmethod
    def server_address(locker):
        """Returns the address and port of the server a locker's server file names.

        :Parameters:
            #. locker (ServerLocker): Any locker that uses the server file.

        :Returns:
            #. address (str): The server IP address.
            #. port (int): The server port.
        """
        _, _, address, port, _ = locker.get_running_server_fingerprint(serverFile=locker.serverFile)
        return address, int(port)

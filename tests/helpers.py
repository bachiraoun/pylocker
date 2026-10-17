"""Shared helpers of the pylocker tests: temporary folders, lockers that stop by themselves, and child processes."""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest

import pylocker
from pylocker import Locker as lockerModule
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

    def make_open_pair(self):
        """Creates a server and a client that both obey stop orders.

        :Returns:
            #. server (ServerLocker): The locker that serves.
            #. client (ServerLocker): The locker connected to it.
        """
        serverFile = self.server_file()
        server = self.make_locker(serverFile, allowRemoteOrders=True)
        client = self.make_client(serverFile, allowRemoteOrders=True)
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


def login_by_hand(connection, reader, password=PASSWORD, uniqueName='raw-client', pid=0, nonce='ab' * 8, name='raw', address='127.0.0.1'):
    """Logs in over a plain connection the way a real client does.

    :Parameters:
        #. connection (socket.socket): The open connection.
        #. reader (file): The file object to read the answers from, made with ``connection.makefile('rb')``.
        #. password (None, str, bytes): The password to prove, or None to answer no challenge.
        #. uniqueName (str): The unique name in the hello message.
        #. pid (int): The process id in the hello message.
        #. nonce (str): The client's random value.
        #. name (str): The name in the hello message.
        #. address (object): The address in the hello message.

    :Returns:
        #. reply (dict): The last message of the server, a welcome or a refusal.
    """
    hello = {'name': name, 'unique_name': uniqueName, 'client_nonce': nonce, 'pid': pid, 'address': address}
    connection.sendall(json.dumps(hello).encode('utf-8') + b'\n')
    challenge = json.loads(reader.readline())
    if challenge.get('action') != 'challenge' or challenge.get('auth_required') is not True or password is None:
        return challenge
    proof = lockerModule._make_proof(lockerModule._to_bytes(password), 'client', nonce, challenge['server_nonce'])
    connection.sendall(json.dumps({'action': 'auth', 'proof': proof}).encode('utf-8') + b'\n')
    return json.loads(reader.readline())


def signed_order(orderName='stop', args=(False,), password=PASSWORD, sentAt=None, orderId='order-1', receivers=None):
    """Builds an order message that carries a proof, like ``stop_remote`` sends.

    :Parameters:
        #. orderName (str): The order name.
        #. args (tuple): The order arguments.
        #. password (str, bytes): The orders password the proof is made with.
        #. sentAt (None, float): Epoch time of the order, now when None.
        #. orderId (str): The order identifier.
        #. receivers (None, list): Unique names the order is for, None for all lockers.

    :Returns:
        #. order (dict): The order message.
    """
    sentAt = time.time() if sentAt is None else sentAt
    args = list(args)
    proof = ServerLocker._order_proof(lockerModule._to_bytes(password), orderName, args, orderId, sentAt, receivers)
    return {'action': 'order', 'order': orderName, 'args': args, 'order_id': orderId, 'sent_at': sentAt,
            'proof': proof, 'receivers': receivers, 'client_unique_name': 'fake-id'}


class FakeServer:
    """
    A server that does the login, grants every lock request, and sends whatever a test asks.

    :Parameters:
        #. serializer (str): The serializer announced in the welcome message.
        #. autoGrant (bool): Whether every acquire request is granted at once.
        #. password (None, str, bytes): The password of the server, None for a server without a login.
        #. proveServer (bool): Whether the welcome message carries a correct proof of the password.
    """

    def __init__(self, serializer='json', autoGrant=True, password=PASSWORD, proveServer=True):
        """Starts listening on a free local port."""
        self.serializer = serializer
        self.autoGrant = autoGrant
        self.password = None if password is None else lockerModule._to_bytes(password)
        self.proveServer = proveServer
        self.received = []
        self.rawLines = []
        self.connection = None
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _login(self, reader):
        """Reads the hello message, runs the challenge and returns the proof for the welcome message.

        :Parameters:
            #. reader (file): The file object to read the client's lines from.

        :Returns:
            #. serverProof (None, str): The proof for the welcome message, None when there is no password.
        """
        hello = json.loads(reader.readline())
        self.rawLines.append(json.dumps(hello))
        if self.password is None:
            self.send({'action': 'challenge', 'auth_required': False})
            return None
        serverNonce = 'cd' * 8
        self.send({'action': 'challenge', 'auth_required': True, 'server_nonce': serverNonce})
        self.rawLines.append(reader.readline().decode('utf-8'))
        if not self.proveServer:
            return 'wrong-proof'
        return lockerModule._make_proof(self.password, 'server', hello['client_nonce'], serverNonce)

    def _serve(self):
        """Accepts one client, logs it in and records what it sends."""
        try:
            connection, _ = self.listener.accept()
        except OSError:
            return
        self.connection = connection
        reader = connection.makefile('rb')
        serverProof = self._login(reader)
        self.send({'action': 'welcome', 'server_name': 'fake', 'server_unique_name': 'fake-id',
                   'server_address': '127.0.0.1', 'server_port': self.port, 'lock_maximum_acquired_time': 60,
                   'server_file': '', 'server_proof': serverProof, 'serializer': self.serializer})
        try:
            for line in reader:
                message = json.loads(line)
                self.received.append(message)
                if self.autoGrant and message.get('action') == 'acquire':
                    self.send({'action': 'acquired', 'request_unique_id': message['request_unique_id']})
        except (OSError, ValueError):
            pass

    def send(self, document):
        """Sends one JSON message to the client.

        :Parameters:
            #. document (dict): The message.
        """
        self.send_raw(json.dumps(document).encode('utf-8') + b'\n')

    def send_raw(self, data):
        """Sends bytes to the client as they are.

        :Parameters:
            #. data (bytes): The bytes to send.
        """
        self.connection.sendall(data)

    def drop_client(self):
        """Closes the connection to the client; shutdown is needed because the reader still holds the socket."""
        self.connection.shutdown(socket.SHUT_RDWR)
        self.connection.close()

    def close(self):
        """Closes the connection and stops listening."""
        for closable in (self.connection, self.listener):
            try:
                closable.shutdown(socket.SHUT_RDWR)
            except (OSError, AttributeError):
                pass
            try:
                closable.close()
            except (OSError, AttributeError):
                pass

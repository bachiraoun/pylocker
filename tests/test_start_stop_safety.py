"""Tests that starting and stopping a locker is safe, and that forks, stop orders and logs behave."""
import os
import socket
import threading
import time
import unittest

from pylocker import AuthenticationError, ServerLocker

from helpers import LockerTestCase, PASSWORD, run_child, wait_until


class TestConcurrentStart(LockerTestCase):
    """Checks that several threads starting the same locker at once end with one working server."""

    def test_many_threads_start_one_locker(self):
        """Starts one locker from six threads, with no error and no thread left behind after the stop."""
        baseline = threading.active_count()
        locker = self.make_locker(self.server_file(), autoconnect=False)
        errors = []

        def start_locker():
            """Starts the locker and keeps the error, if any."""
            try:
                locker.start()
            except Exception as error:
                errors.append(error)

        starters = [threading.Thread(target=start_locker) for _ in range(6)]
        for starter in starters:
            starter.start()
        for starter in starters:
            starter.join(30)
        self.assertEqual(errors, [])
        self.assertTrue(locker.isServer)
        locker.stop()
        self.assertTrue(wait_until(lambda: threading.active_count() <= baseline, seconds=5))

    def test_failed_start_leaves_no_thread(self):
        """Raises on each wrong-password start and leaves no event loop thread running."""
        server = self.make_locker(self.server_file())
        client = self.make_client(server.serverFile, password='wrong', autoconnect=False, sharedLoop=False)
        baseline = threading.active_count()
        for _ in range(3):
            with self.assertRaises(AuthenticationError):
                client.start()
        self.assertTrue(wait_until(lambda: threading.active_count() <= baseline, seconds=5))


class TestOwnedLocks(LockerTestCase):
    """Checks that a locker does not list locks that the server no longer holds."""

    def test_server_forgets_its_locks_when_it_stops(self):
        """Gives an empty list of owned locks after the server stopped."""
        server = self.make_locker(self.server_file())
        self.assertTrue(server.acquire_lock(self.lock_path('a'), timeout=2)[0])
        server.stop()
        self.assertEqual(server.ownedLocks, {})

    def test_client_forgets_its_locks_when_it_loses_the_server(self):
        """Gives an empty list of owned locks once the connection to the server ended."""
        server, client = self.make_pair()
        self.assertTrue(client.acquire_lock(self.lock_path('a'), timeout=2)[0])
        server.stop()
        self.assertTrue(wait_until(lambda: client.ownedLocks == {}))


FORK_CODE = """
serverFile = {serverFile!r}
locker = ServerLocker(PASSWORD, serverFile=serverFile, connectTimeout=5)
pid = os.fork()
if pid == 0:
    result = locker.acquire_lock('x', timeout=1, retries=0)
    with open({resultFile!r}, 'w') as fileHandle:
        fileHandle.write(repr(result))
    sys.exit(0)
os.waitpid(pid, 0)
print(locker.isServer)
print(open(serverFile).read().strip() != '')
"""


@unittest.skipUnless(hasattr(os, 'fork'), 'needs os.fork')
class TestForkedCopy(LockerTestCase):
    """Checks that a forked copy of a locker neither waits for its timeout nor clears the parent's file."""

    def test_child_gets_code_2_and_keeps_the_server_file(self):
        """Answers "not started" in the child and leaves the fingerprint in the server file at its exit."""
        resultFile = os.path.join(self.tempFolder, 'result.txt')
        completed = run_child(FORK_CODE.format(serverFile=self.server_file(), resultFile=resultFile))
        self.assertEqual(completed.stdout.split(), ['True', 'True'], completed.stderr[-300:])
        with open(resultFile) as fileHandle:
            self.assertEqual(fileHandle.read(), '(False, 2)')


class TestOrderMemory(LockerTestCase):
    """Checks that a stop order that was obeyed cannot be used again."""

    def test_obeyed_order_is_refused_after_a_restart(self):
        """Refuses the same order after the locker was stopped and started again."""
        ordersPassword = PASSWORD.encode('utf-8')
        locker = self.make_locker(False, autoconnect=False, allowRemoteOrders=True)
        orderId, sentAt = 'order-1', time.time()
        order = {'order': 'stop', 'args': [False], 'order_id': orderId, 'sent_at': sentAt, 'receivers': None,
                 'proof': ServerLocker._order_proof(ordersPassword, 'stop', [False], orderId, sentAt, None)}
        locker.start()
        self.assertIsNone(locker._order_problem(order, ordersPassword))
        locker.stop()
        locker.start()
        self.assertEqual(locker._order_problem(order, ordersPassword), 'it was already received')


class TestLogs(LockerTestCase):
    """Checks what the server logs about broken connections and failed tasks."""

    def test_garbage_connections_are_logged_up_to_a_limit(self):
        """Logs 20 client errors, then one notice that the next ones are not logged."""
        server = self.make_locker(self.server_file())
        messages = []
        server._error = messages.append
        for _ in range(30):
            with socket.create_connection(('127.0.0.1', server._serverPort), timeout=2) as connection:
                connection.sendall(b'\xff\xfe\n')
        self.assertTrue(wait_until(lambda: any('are not logged' in message for message in messages)))
        time.sleep(0.5)
        handlerMessages = [message for message in messages if message.startswith('Client handler error')]
        self.assertEqual(len(handlerMessages), 20)
        self.assertEqual(len([message for message in messages if 'are not logged' in message]), 1)

    def test_background_task_failure_is_logged(self):
        """Logs a critical message naming the task that ended with an error."""
        server = self.make_locker(self.server_file())
        messages = []
        server._critical = messages.append

        class FailedTask:
            """Stands for a background task that ended with an error."""

            def cancelled(self):
                """Tells that the task was not cancelled."""
                return False

            def exception(self):
                """Returns the error the task ended with."""
                return RuntimeError('boom')

            def get_coro(self):
                """Returns the function the task ran."""
                return server._queue_processor

        server._on_background_task_done(FailedTask())
        self.assertEqual(len(messages), 1)
        self.assertIn('_queue_processor', messages[0])
        self.assertIn('boom', messages[0])


if __name__ == '__main__':
    unittest.main()

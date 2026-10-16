"""Tests the login: proofs instead of passwords, running without a password, blocking after failed logins, and signed orders."""
import pickle
import socket
import threading
import time
import unittest
from unittest import mock

from pylocker import AuthenticationError, ServerLocker
from pylocker import Locker as lockerModule

from helpers import FakeServer, LockerTestCase, PASSWORD, login_by_hand, signed_order, wait_until

SECRET = 'correct-horse-battery'


class TestProofInsteadOfPassword(LockerTestCase):
    """Checks what a client sends and what it accepts from the server."""

    def connect_to(self, fake, **kwargs):
        """Starts a client against a fake server.

        :Parameters:
            #. fake (FakeServer): The server to connect to.
            #. kwargs (dict): Arguments of ``ServerLocker``, replacing the defaults.

        :Returns:
            #. client (ServerLocker): The client; connected unless the login failed.
        """
        arguments = {'password': SECRET, 'serverFile': False, 'autoconnect': False, 'allowServing': False,
                     'connectTimeout': 3}
        arguments.update(kwargs)
        client = self.track(ServerLocker(**arguments))
        client.start(address='127.0.0.1', port=fake.port, ntrials=1)
        return client

    def test_password_is_never_sent(self):
        """Sends no trace of the password to the server."""
        fake = FakeServer(password=SECRET)
        self.addCleanup(fake.close)
        client = self.connect_to(fake)
        self.assertTrue(client.isClient)
        self.assertTrue(len(fake.rawLines) >= 2)
        self.assertNotIn(SECRET, ''.join(fake.rawLines))

    def test_proof_is_different_each_time(self):
        """Sends a different proof on every login."""
        proofs = set()
        for _ in range(3):
            fake = FakeServer(password=SECRET)
            self.addCleanup(fake.close)
            self.assertTrue(self.connect_to(fake).isClient)
            proofs.add(fake.rawLines[1])
        self.assertEqual(len(proofs), 3)

    def test_server_that_cannot_prove_the_password_is_refused(self):
        """Raises AuthenticationError when the welcome message carries a wrong proof."""
        fake = FakeServer(password=SECRET, proveServer=False)
        self.addCleanup(fake.close)
        with self.assertRaisesRegex(AuthenticationError, 'did not prove'):
            self.connect_to(fake)

    def test_server_that_asks_for_no_password_is_refused_by_a_client_with_one(self):
        """Raises AuthenticationError instead of talking to a server that cannot know the password."""
        fake = FakeServer(password=None)
        self.addCleanup(fake.close)
        with self.assertRaisesRegex(AuthenticationError, 'does not ask for a password'):
            self.connect_to(fake)

    def test_client_without_a_password_is_refused_by_a_server_that_asks(self):
        """Raises AuthenticationError when the server asks for a password and the client has none."""
        fake = FakeServer(password=SECRET)
        self.addCleanup(fake.close)
        with self.assertRaisesRegex(AuthenticationError, 'requires a password'):
            self.connect_to(fake, password=None)

    def test_wrong_proof_is_refused_by_a_real_server(self):
        """Refuses a client that proves a different password."""
        server = self.make_locker(self.server_file(), password=SECRET)
        with self.assertRaises(AuthenticationError):
            ServerLocker('other', serverFile=server.serverFile, connectTimeout=3, allowServing=False)


class TestWithoutPassword(LockerTestCase):
    """Checks lockers that run without a login."""

    def test_clients_and_server_lock_without_a_password(self):
        """Locks through a server that has no password."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile, password=None)
        client = self.make_client(serverFile, password=None)
        self.assertTrue(server.isServer and client.isClient)
        path = self.lock_path('np')
        acquired, lockId = client.acquire_lock(path, timeout=2)
        self.assertTrue(acquired)
        self.assertEqual(server.acquire_lock(path, timeout=0.3, retries=0), (False, 0))
        client.release_lock(lockId)
        self.assertTrue(server.acquire_lock(path, timeout=2)[0])

    def test_client_with_a_password_does_not_join_a_server_without_one(self):
        """Raises AuthenticationError for a client that has a password when the server has none."""
        serverFile = self.server_file()
        self.make_locker(serverFile, password=None)
        with self.assertRaises(AuthenticationError):
            ServerLocker(PASSWORD, serverFile=serverFile, connectTimeout=3, allowServing=False)

    def test_client_without_a_password_does_not_join_a_server_with_one(self):
        """Raises AuthenticationError for a client that has no password when the server has one."""
        serverFile = self.server_file()
        self.make_locker(serverFile)
        with self.assertRaises(AuthenticationError):
            ServerLocker(None, serverFile=serverFile, connectTimeout=3, allowServing=False)

    def test_remote_clients_need_a_password(self):
        """Refuses to combine no password with serving other computers."""
        with self.assertRaises(ValueError):
            ServerLocker(None, serverFile=False, autoconnect=False, allowRemoteClients=True)
        locker = ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowRemoteClients=True)
        with self.assertRaises(ValueError):
            locker.set_password(None)

    def test_password_must_be_text_bytes_or_none(self):
        """Raises TypeError for a password of another type."""
        with self.assertRaises(TypeError):
            ServerLocker(5, serverFile=False, autoconnect=False)

    def test_orders_need_a_password(self):
        """Refuses to switch orders on when there is no password at all to check them with."""
        locker = ServerLocker(None, serverFile=False, autoconnect=False)
        with self.assertRaises(ValueError):
            locker.allow_receiving_remote_orders(True)
        locker.allow_receiving_remote_orders(True, password='orders-only')

    def test_order_is_ignored_when_there_is_no_orders_password(self):
        """Leaves a server without any password running when a client sends a stop order."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile, password=None)
        client = self.make_client(serverFile, password=None)
        client.stop_remote(password='anything')
        time.sleep(1.5)
        self.assertTrue(server.isServer)

    def test_order_with_its_own_password_works_without_a_login(self):
        """Obeys a stop order that proves the orders password even though the server has no login."""
        serverFile = self.server_file()
        server = self.make_locker(serverFile, password=None)
        server.allow_receiving_remote_orders(True, password='orders-only')
        client = self.make_client(serverFile, password=None)
        client.stop_remote(password='orders-only')
        self.assertTrue(wait_until(lambda: not server.isServer))

    def test_copy_of_a_locker_never_starts_without_a_password_by_accident(self):
        """Refuses to start a copy until it is given a password or told to have none."""
        original = ServerLocker(None, serverFile=False, autoconnect=False)
        clone = pickle.loads(pickle.dumps(original))
        self.assertIsNone(clone.password)
        with self.assertRaisesRegex(AuthenticationError, 'no password'):
            clone.start()
        with self.assertRaises(AuthenticationError):
            clone.connect('127.0.0.1', 3000)


class TestBlocking(LockerTestCase):
    """Checks that a sender is blocked after too many failed logins."""

    def setUp(self):
        """Creates a server that blocks for two seconds after three failed logins."""
        super().setUp()
        self.server = self.make_locker(self.server_file(), blockSeconds=2)
        self.address = self.server_address(self.server)

    def login(self, pid, password=PASSWORD):
        """Logs in by hand as a process with the given id.

        :Parameters:
            #. pid (int): The process id the hello message reports.
            #. password (str): The password to prove.

        :Returns:
            #. reply (dict): The welcome or the refusal.
        """
        with socket.create_connection(self.address, timeout=3) as connection:
            return login_by_hand(connection, connection.makefile('rb'), password=password, pid=pid,
                                 uniqueName=f'raw-{pid}')

    def fail(self, pid, count):
        """Fails to log in several times as a process.

        :Parameters:
            #. pid (int): The process id the hello message reports.
            #. count (int): The number of failed logins.
        """
        for _ in range(count):
            self.assertEqual(self.login(pid, password='WRONG')['reason'], 'bad password')

    def test_blocked_after_three_failures(self):
        """Refuses even the right password once the limit of failures is reached."""
        self.fail(111, 3)
        reply = self.login(111)
        self.assertEqual((reply['action'], reply['reason']), ('error', 'blocked'))
        self.assertEqual(list(self.server.blockedClients), ["local process '111'"])

    def test_two_failures_do_not_block(self):
        """Accepts the right password after two failures."""
        self.fail(112, 2)
        self.assertEqual(self.login(112)['action'], 'welcome')

    def test_other_senders_are_not_blocked(self):
        """Lets another process log in while one is blocked."""
        self.fail(113, 3)
        self.assertEqual(self.login(114)['action'], 'welcome')

    def test_success_resets_the_count(self):
        """Counts only failures that follow each other."""
        self.fail(115, 2)
        self.assertEqual(self.login(115)['action'], 'welcome')
        self.fail(115, 2)
        self.assertEqual(self.login(115)['action'], 'welcome')

    def test_block_ends_by_itself(self):
        """Accepts the sender again once the block time has passed."""
        self.fail(116, 3)
        self.assertEqual(self.login(116)['reason'], 'blocked')
        time.sleep(2.3)
        self.assertEqual(self.login(116)['action'], 'welcome')
        self.assertEqual(self.server.blockedClients, {})

    def test_unblock_one_client(self):
        """Lifts the block of one client and leaves the others blocked."""
        self.fail(117, 3)
        self.fail(118, 3)
        self.server.unblock_client("local process '117'")
        self.assertEqual(self.login(117)['action'], 'welcome')
        self.assertEqual(self.login(118)['reason'], 'blocked')

    def test_unblock_all(self):
        """Lifts every block."""
        self.fail(119, 3)
        self.server.unblock_client()
        self.assertEqual(self.login(119)['action'], 'welcome')

    def test_real_client_is_told_it_is_blocked(self):
        """Raises AuthenticationError that names the block for a real client."""
        serverFile = self.server.serverFile
        for _ in range(3):
            with self.assertRaisesRegex(AuthenticationError, 'bad password'):
                ServerLocker('WRONG', serverFile=serverFile, connectTimeout=3, allowServing=False)
        with self.assertRaisesRegex(AuthenticationError, 'blocked'):
            ServerLocker(PASSWORD, serverFile=serverFile, connectTimeout=3, allowServing=False)
        self.server.unblock_client()
        self.assertTrue(self.make_client(serverFile).isClient)

    def test_hello_without_a_random_value_counts_as_a_failure(self):
        """Counts an old-style hello among the failures."""
        for _ in range(3):
            with socket.create_connection(self.address, timeout=3) as connection:
                connection.sendall(b'{"pid": 120, "password": "pw"}\n')
                connection.makefile('rb').readline()
        self.assertEqual(self.login(120)['reason'], 'blocked')

    def test_blocking_can_be_switched_off(self):
        """Never blocks when the limit is None."""
        server = self.make_locker(self.server_file('off.lock'), maxAuthFailures=None)
        self.address = self.server_address(server)
        self.fail(121, 6)
        self.assertEqual(self.login(121)['action'], 'welcome')
        self.assertEqual(server.blockedClients, {})

    def test_old_failures_are_forgotten(self):
        """Starts counting again when the last failure is older than the block time."""
        self.fail(122, 2)
        time.sleep(2.3)
        self.fail(122, 2)
        self.assertEqual(self.login(122)['action'], 'welcome')

    def test_table_of_senders_has_a_limit(self):
        """Keeps the table of failed senders from growing without a limit."""
        with mock.patch.object(lockerModule, '_MAX_TRACKED_CLIENTS', 5):
            for index in range(30):
                self.server._record_auth_failure(f'sender {index}')
        self.assertLessEqual(len(self.server._authFailures), 5)

    def test_remote_senders_are_identified_by_address(self):
        """Names a sender from another computer by its address and a local one by its process id."""
        class FakeWriter:
            """Answers only the question about the peer."""

            def __init__(self, peer):
                self.peer = peer

            def get_extra_info(self, name):
                return self.peer

        self.assertEqual(ServerLocker._block_key(FakeWriter(('10.1.2.3', 4000)), {'pid': 5}), 'address 10.1.2.3')
        self.assertEqual(ServerLocker._block_key(FakeWriter(('127.0.0.1', 4000)), {'pid': 5}), "local process '5'")
        self.assertEqual(ServerLocker._block_key(FakeWriter(None), {}), 'address unknown')

    def test_bad_settings_are_refused(self):
        """Raises ValueError for a limit that is not a whole number of at least 1, or a block time that is not positive."""
        for kwargs in ({'maxAuthFailures': 0}, {'maxAuthFailures': True}, {'maxAuthFailures': 'x'},
                       {'blockSeconds': 0}, {'blockSeconds': -5}):
            with self.subTest(kwargs):
                with self.assertRaises(ValueError):
                    ServerLocker(PASSWORD, serverFile=False, autoconnect=False, **kwargs)


class TestSignedOrders(LockerTestCase):
    """Checks which stop orders a locker obeys."""

    def setUp(self):
        """Creates a locker whose stop is replaced by a counter."""
        super().setUp()
        self.locker = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False))
        self.stops = []
        self.stopped = threading.Event()

        def count(killPid):
            self.stops.append(killPid)
            self.stopped.set()

        patcher = mock.patch.object(self.locker, '_run_stop_order', count)
        patcher.start()
        self.addCleanup(patcher.stop)

    def obeys(self, order):
        """Handles an order and tells whether the locker obeyed it.

        :Parameters:
            #. order (dict): The order message.

        :Returns:
            #. isObeyed (bool): Whether the stop ran.
        """
        self.stopped.clear()
        self.locker._handle_order(order)
        return self.stopped.wait(0.5)

    def test_genuine_order_is_obeyed(self):
        """Obeys an order with a right proof."""
        self.assertTrue(self.obeys(signed_order()))

    def test_kill_flag_is_passed_on(self):
        """Passes the kill flag of the order to the stop."""
        self.assertTrue(self.obeys(signed_order(args=(True,))))
        self.assertEqual(self.stops, [True])

    def test_replayed_order_is_ignored(self):
        """Ignores an order that was already received."""
        order = signed_order()
        self.assertTrue(self.obeys(order))
        self.assertFalse(self.obeys(order))
        self.assertTrue(self.obeys(signed_order(orderId='order-2')))

    def test_old_and_future_orders_are_ignored(self):
        """Ignores an order that is more than the allowed age old, or that comes from the future."""
        self.assertFalse(self.obeys(signed_order(sentAt=time.time() - 120, orderId='old')))
        self.assertFalse(self.obeys(signed_order(sentAt=time.time() + 120, orderId='future')))

    def test_changed_order_is_ignored(self):
        """Ignores an order whose arguments were changed after it was signed."""
        order = signed_order(args=(False,))
        order['args'] = [True]
        self.assertFalse(self.obeys(order))

    def test_wrong_password_is_ignored(self):
        """Ignores an order signed with another password."""
        self.assertFalse(self.obeys(signed_order(password='other')))

    def test_order_without_a_proof_is_ignored(self):
        """Ignores an order that lacks the proof, the identifier or the time, or has them with a wrong type."""
        for field in ('proof', 'order_id', 'sent_at'):
            with self.subTest(field):
                order = signed_order(orderId=f'missing-{field}')
                del order[field]
                self.assertFalse(self.obeys(order))
        order = signed_order(orderId='boolean-time')
        order['sent_at'] = True
        self.assertFalse(self.obeys(order))

    def test_forged_order_does_not_use_up_an_identifier(self):
        """Still obeys the genuine order after a forged one that used the same identifier."""
        self.assertFalse(self.obeys(signed_order(password='other', orderId='same')))
        self.assertTrue(self.obeys(signed_order(orderId='same')))

    def test_locker_without_a_password_ignores_orders_quietly(self):
        """Ignores an order, without raising, when the locker has no password to check it with."""
        locker = self.track(ServerLocker(None, serverFile=False, autoconnect=False))
        with mock.patch.object(locker, '_run_stop_order') as stop:
            locker._handle_order(signed_order())
            time.sleep(0.3)
        stop.assert_not_called()

    def test_order_has_no_password_in_it(self):
        """Carries no password in the order a locker sends."""
        sent = []
        with mock.patch.object(self.locker, '_run_sync', lambda coroutine, timeout=None: sent.append(coroutine)):
            self.locker._writer = object()
            with mock.patch.object(ServerLocker, 'isClient', new_callable=mock.PropertyMock, return_value=True):
                with mock.patch.object(self.locker, '_write_message', lambda writer, order: sent.append(order)):
                    self.locker.stop_remote(password=SECRET)
        order = [item for item in sent if isinstance(item, dict)][0]
        self.assertNotIn('password', order)
        self.assertNotIn(SECRET, repr(order))
        self.assertEqual({'order_id', 'sent_at', 'proof'} - set(order), set())


if __name__ == '__main__':
    unittest.main()

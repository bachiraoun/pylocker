"""Tests the orders that stop other lockers, from a client to a server and back, and across processes."""
import time

from pylocker import ServerLocker

from helpers import LockerTestCase, spawn_child, wait_until


class TestStopRemote(LockerTestCase):
    """Checks who may stop whom."""

    def test_client_stops_the_server(self):
        """Stops the server when a client sends a stop order with the right password."""
        server, client = self.make_pair()
        client.stop_remote(password='pw')
        self.assertTrue(wait_until(lambda: not server.isServer))

    def test_wrong_password_is_ignored(self):
        """Leaves the server running and stores no message when the order has the wrong password."""
        server, client = self.make_pair()
        client.stop_remote(password='WRONG')
        time.sleep(1.5)
        self.assertTrue(server.isServer)
        self.assertEqual(server.messages, [])
        self.assertEqual(client.messages, [])

    def test_orders_can_be_switched_off(self):
        """Ignores a stop order while orders are switched off."""
        server, client = self.make_pair()
        server.allow_receiving_remote_orders(allow=False)
        client.stop_remote(password='pw')
        time.sleep(1.5)
        self.assertTrue(server.isServer)

    def test_orders_can_be_switched_back_on(self):
        """Obeys a stop order again once orders are switched on."""
        server, client = self.make_pair()
        server.allow_receiving_remote_orders(allow=False)
        server.allow_receiving_remote_orders(allow=True)
        client.stop_remote(password='pw')
        self.assertTrue(wait_until(lambda: not server.isServer))

    def test_server_stops_its_client(self):
        """Stops a client when the server sends a stop order."""
        server, client = self.make_pair()
        server.stop_remote(password='pw')
        self.assertTrue(wait_until(lambda: not client.isClient))

    def test_stop_self_after_the_order(self):
        """Stops the sender too when stopSelf is set."""
        server, client = self.make_pair()
        client.stop_remote(password='pw', stopSelf=True)
        self.assertFalse(client.isClient)
        self.assertTrue(wait_until(lambda: not server.isServer))


class TestStopRemoteArguments(LockerTestCase):
    """Checks the arguments of stop_remote."""

    def test_bad_arguments(self):
        """Raises TypeError for a password, killRemotePID or receivers of the wrong type."""
        _, client = self.make_pair()
        for label, kwargs in (('password', {'password': 5}), ('killRemotePID', {'password': 'pw', 'killRemotePID': 1}),
                              ('receivers', {'password': 'pw', 'receivers': 'everyone'})):
            with self.subTest(label):
                with self.assertRaises(TypeError):
                    client.stop_remote(**kwargs)

    def test_receivers_limit_who_is_stopped(self):
        """Stops only the listed receivers."""
        server, first = self.make_pair()
        second = self.make_client(server.serverFile)
        server.stop_remote(password='pw', receivers=[first.uniqueName])
        self.assertTrue(wait_until(lambda: not first.isClient))
        time.sleep(0.5)
        self.assertTrue(second.isClient)

    def test_not_started_locker_only_warns(self):
        """Does nothing, and does not raise, on a locker that is not running."""
        idle = ServerLocker('pw', serverFile=False, autoconnect=False)
        idle.stop_remote(password='pw')
        self.assertFalse(idle.isServer or idle.isClient)


class TestKillRemoteProcess(LockerTestCase):
    """Checks that an order can end the whole process of a server in another process."""

    def test_order_kills_the_server_process(self):
        """Ends the process of the server when killRemotePID is set."""
        serverFile = self.server_file()
        code = f"""
        locker = ServerLocker(PASSWORD, serverFile={serverFile!r}, connectTimeout=5)
        print('READY', os.getpid(), flush=True)
        time.sleep(60)
        """
        child = spawn_child(code)
        self.addCleanup(child.kill)
        child.stdout.readline()
        client = self.make_client(serverFile)
        self.assertTrue(client.isClient)
        client.stop_remote(password='pw', killRemotePID=True, stopSelf=True)
        self.assertTrue(wait_until(lambda: child.poll() is not None, seconds=8))
        self.assertFalse(client.isClient)

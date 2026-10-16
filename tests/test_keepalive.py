"""Tests that silent connections are probed, and that a failed connection ends its read loop instead of spinning."""
import asyncio
import errno
import logging
import socket
import time
import unittest
from unittest import mock

from pylocker import ServerLocker
from pylocker import Locker as lockerModule

from helpers import FakeServer, LockerTestCase, PASSWORD, wait_until
from test_hostile import RawClient, acquire_request


def option_value(connectionSocket, name, level=socket.IPPROTO_TCP):
    """Reads a socket option by name, or None when the system does not offer it.

    :Parameters:
        #. connectionSocket (socket.socket): The socket to read from.
        #. name (str): The option name in the socket module, e.g. ``TCP_KEEPINTVL``.
        #. level (int): The protocol level of the option.

    :Returns:
        #. value (None, int): The option value, or None when the option does not exist here.
    """
    option = getattr(socket, name, None)
    return None if option is None else connectionSocket.getsockopt(level, option)


class TestEnableKeepalive(unittest.TestCase):
    """Checks the function that switches the probes on."""

    @unittest.skipIf(hasattr(socket, 'SIO_KEEPALIVE_VALS'), 'Windows sets the probes in another way')
    def test_probes_are_switched_on_with_the_chosen_times(self):
        """Switches the probes on and sets the idle time, the interval and the number of probes."""
        with socket.socket() as probe:
            lockerModule._enable_keepalive(probe)
            self.assertEqual(probe.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0, True)
            idle = option_value(probe, 'TCP_KEEPIDLE') or option_value(probe, 'TCP_KEEPALIVE')
            self.assertEqual(idle, lockerModule._KEEPALIVE_IDLE_SECONDS)
            interval = option_value(probe, 'TCP_KEEPINTVL')
            if interval is not None:
                self.assertEqual(interval, lockerModule._KEEPALIVE_INTERVAL_SECONDS)
            count = option_value(probe, 'TCP_KEEPCNT')
            if count is not None:
                self.assertEqual(count, lockerModule._KEEPALIVE_PROBES)

    def test_missing_or_broken_sockets_are_not_an_error(self):
        """Does nothing for no socket, and does not raise when the system refuses an option."""
        lockerModule._enable_keepalive(None)
        broken = mock.Mock()
        broken.setsockopt.side_effect = OSError(errno.EINVAL, 'invalid')
        broken.ioctl.side_effect = OSError(errno.EINVAL, 'invalid')
        lockerModule._enable_keepalive(broken)
        closed = socket.socket()
        closed.close()
        lockerModule._enable_keepalive(closed)


class TestConnectionsAreProbed(LockerTestCase):
    """Checks that both ends of a connection ask for probes."""

    def test_server_and_client_sockets_have_keepalive(self):
        """Turns the probes on for the accepted socket of the server and for the socket of the client."""
        server, client = self.make_pair()
        raw = RawClient(server._serverPort, uniqueName='raw')
        self.addCleanup(raw.close)
        accepted = [record['writer'].get_extra_info('socket') for record in server._clientsLUT.values()]
        self.assertEqual(len(accepted), 2)
        sockets = accepted + [client._writer.get_extra_info('socket')]
        for connectionSocket in sockets:
            self.assertNotEqual(connectionSocket.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 0)


class TestFailedConnectionEndsTheLoop(LockerTestCase):
    """Checks that a connection that failed ends its read loop at once, instead of failing again and again."""

    def test_client_stops_reading_after_a_lost_connection(self):
        """Disconnects the client, with a handful of reads at most, for every kind of lost connection."""
        errors = {'timed out': OSError(errno.ETIMEDOUT, 'Operation timed out'),
                  'aborted': ConnectionAbortedError('aborted'),
                  'unreachable': OSError(errno.EHOSTUNREACH, 'No route to host'),
                  'time-out error': TimeoutError('timed out')}
        for label, error in errors.items():
            with self.subTest(label):
                fake = FakeServer()
                self.addCleanup(fake.close)
                client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                                 connectTimeout=3))
                client.start(address='127.0.0.1', port=fake.port, connectAttempts=1)
                self.assertTrue(client.isClient)
                calls = []

                async def failing(reader, error=error):
                    calls.append(1)
                    await asyncio.sleep(0)
                    raise error

                client._read_message = failing
                fake.send({'action': 'noop'})
                self.assertTrue(wait_until(lambda: not client.isClient, seconds=3))
                self.assertLessEqual(len(calls), 3)

    def test_garbled_line_is_still_skipped(self):
        """Keeps reading after a line that is not a message, since that is not a lost connection."""
        fake = FakeServer()
        self.addCleanup(fake.close)
        client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                         connectTimeout=3))
        client.start(address='127.0.0.1', port=fake.port, connectAttempts=1)
        fake.send_raw(b'not json\n')
        time.sleep(0.4)
        self.assertTrue(client.isClient)

    def test_server_drops_a_peer_whose_connection_failed(self):
        """Drops a peer whose connection failed, with a handful of reads at most, and logs a warning."""
        logger = logging.getLogger('pylocker.test.keepalive')
        server = self.make_locker(self.server_file(), logger=logger)
        raw = RawClient(server._serverPort, uniqueName='peer')
        self.addCleanup(raw.close)
        calls = []

        async def failing(reader, timeout):
            calls.append(1)
            reader.set_exception(OSError(errno.ETIMEDOUT, 'Operation timed out'))
            raise asyncio.TimeoutError()

        with mock.patch.object(server, '_read_message_timeout', failing):
            with self.assertLogs(logger, level='WARNING') as captured:
                raw.send({'action': 'noop'})
                self.assertTrue(wait_until(lambda: 'peer' not in server._clientsLUT, seconds=3))
        self.assertLessEqual(len(calls), 3)
        self.assertTrue(any('lost' in line for line in captured.output))

    def test_server_keeps_a_peer_that_is_only_quiet(self):
        """Keeps a peer on a plain wait time-out, and serves it afterwards."""
        server = self.make_locker(self.server_file())
        raw = RawClient(server._serverPort, uniqueName='quiet')
        self.addCleanup(raw.close)
        original = server._read_message_timeout
        quietWaits = []

        async def sometimes_quiet(reader, timeout):
            if len(quietWaits) < 3:
                quietWaits.append(1)
                raise asyncio.TimeoutError()
            return await original(reader, timeout)

        with mock.patch.object(server, '_read_message_timeout', sometimes_quiet):
            raw.send(acquire_request(path=[self.lock_path('quiet')]))
            self.assertEqual(raw.read()['action'], 'acquired')
        self.assertEqual(len(quietWaits), 3)
        self.assertIn('quiet', server._clientsLUT)


if __name__ == '__main__':
    unittest.main()

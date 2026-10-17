"""Tests that a stuck file lock cannot hold up the loop that serves every client."""
import time
import unittest

try:
    import fcntl
except ImportError:
    fcntl = None

from pylocker import Locker as lockerModule

from helpers import LockerTestCase, wait_until


@unittest.skipIf(fcntl is None, 'needs file locks')
class TestHeartbeatFileWork(LockerTestCase):
    """Checks that the work of the heartbeat on the server file does not stop the server from answering."""

    def test_stuck_lock_holder_does_not_slow_the_clients(self):
        """Answers lock requests at once while another process holds the election lock."""
        server, client = self.make_pair()
        lockFile = open(server.serverFile + lockerModule._LOCK_FILE_SUFFIX, 'a')
        self.addCleanup(lockFile.close)
        fcntl.flock(lockFile, fcntl.LOCK_EX)
        slowest = 0.0
        # Longer than the heartbeat interval, so at least one refresh of the file runs while the lock is held
        end = time.time() + lockerModule._HEARTBEAT_INTERVAL * 2 + 1
        index = 0
        while time.time() < end:
            start = time.time()
            self.assertTrue(client.acquire_lock(self.lock_path(f'p{index}'), timeout=5, retries=0)[0])
            slowest = max(slowest, time.time() - start)
            index += 1
            time.sleep(0.1)
        fcntl.flock(lockFile, fcntl.LOCK_UN)
        self.assertLess(slowest, 1.0)

    def test_heartbeat_still_refreshes_the_file(self):
        """Keeps the server file fresh, so the server stays the server."""
        server = self.make_locker(self.server_file())
        first = server.get_running_server_fingerprint(serverFile=server.serverFile)[1]
        self.assertTrue(wait_until(
            lambda: server.get_running_server_fingerprint(serverFile=server.serverFile)[1] != first,
            seconds=lockerModule._HEARTBEAT_INTERVAL * 2 + 2))
        self.assertTrue(server.isServer)


if __name__ == '__main__':
    unittest.main()

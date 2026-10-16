"""Tests that a request for several paths is not overtaken again and again by requests for one of them."""
import threading
import time
import unittest
from unittest import mock

from pylocker import Locker as lockerModule

from helpers import LockerTestCase


class TestRequestOrder(LockerTestCase):
    """Checks which waiting request gets a path first."""

    def setUp(self):
        """Creates a server, three clients, and a lock on one of two paths held by the first client."""
        super().setUp()
        self.server, self.holder = self.make_pair()
        self.waiter = self.make_client(self.server.serverFile)
        self.latecomer = self.make_client(self.server.serverFile)
        acquired, self.holdId = self.holder.acquire_lock('second')
        self.assertTrue(acquired)

    def start_waiter(self):
        """Starts a request for both paths that cannot be granted yet, and returns its outcome list.

        :Returns:
            #. outcome (list): Holds the result of the request once it is over.
        """
        outcome = []
        thread = threading.Thread(target=lambda: outcome.append(self.waiter.acquire_lock(
            ['first', 'second'], timeout=10, retries=0)), daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        time.sleep(0.5)
        return outcome

    def test_newer_request_does_not_take_a_path_an_older_one_waits_for(self):
        """Keeps the free path for the older request, which is granted as soon as the other path is free."""
        outcome = self.start_waiter()
        acquired, _ = self.latecomer.acquire_lock('first', timeout=1, retries=0)
        self.assertFalse(acquired)
        self.holder.release_lock(self.holdId)
        deadline = time.time() + 5
        while not outcome and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(outcome and outcome[0][0])

    def test_requests_for_other_paths_are_not_held_back(self):
        """Grants a newer request at once when none of its paths is wanted by an older request."""
        self.start_waiter()
        started = time.time()
        acquired, _ = self.latecomer.acquire_lock('unrelated', timeout=2, retries=0)
        self.assertTrue(acquired)
        self.assertLess(time.time() - started, 1)

    def test_only_the_oldest_requests_keep_their_paths(self):
        """Stops reserving paths beyond the window, so thousands of waiting requests stay cheap."""
        with mock.patch.object(lockerModule, '_FAIRNESS_WINDOW', 0):
            self.start_waiter()
            acquired, _ = self.latecomer.acquire_lock('first', timeout=1, retries=0)
        self.assertTrue(acquired)

    def test_request_that_gave_up_keeps_nothing(self):
        """Lets a newer request take the path once the older request has timed out."""
        outcome = []
        thread = threading.Thread(target=lambda: outcome.append(self.waiter.acquire_lock(
            ['first', 'second'], timeout=0.5, retries=0)), daemon=True)
        thread.start()
        thread.join(5)
        self.assertFalse(outcome[0][0])
        acquired, _ = self.latecomer.acquire_lock('first', timeout=2, retries=0)
        self.assertTrue(acquired)


if __name__ == '__main__':
    unittest.main()

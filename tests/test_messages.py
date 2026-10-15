"""Tests publishing messages between lockers: receivers, expiry, ``toSelf`` and ``unique``."""
import time

from helpers import LockerTestCase, wait_until


class TestMessages(LockerTestCase):
    """Checks who receives a published message and for how long."""

    def setUp(self):
        """Creates a server and two clients."""
        super().setUp()
        serverFile = self.server_file()
        self.server = self.make_locker(serverFile, name='srv')
        self.first = self.make_client(serverFile, name='c1')
        self.second = self.make_client(serverFile, name='c2')

    def holders(self, message):
        """Tells which lockers hold a message.

        :Parameters:
            #. message (str): The message key.

        :Returns:
            #. holders (dict): ``server``, ``first`` and ``second`` mapped to True or False.
        """
        return {'server': self.server.has_message(message), 'first': self.first.has_message(message),
                'second': self.second.has_message(message)}

    def test_message_reaches_everyone_by_default(self):
        """Gives a message published by a client to the server and the other client."""
        self.first.publish_message('all')
        self.assertTrue(wait_until(lambda: self.second.has_message('all')))
        self.assertEqual(self.holders('all'), {'server': True, 'first': True, 'second': True})

    def test_receivers_limit_who_gets_the_message(self):
        """Gives the message only to the named receivers, even if the publisher is not one of them."""
        self.first.publish_message('only2', receivers=[self.second.uniqueName], toSelf=True)
        self.assertTrue(wait_until(lambda: self.second.has_message('only2')))
        time.sleep(0.3)
        self.assertEqual(self.holders('only2'), {'server': False, 'first': False, 'second': True})

    def test_publisher_listed_as_receiver_gets_it_with_to_self(self):
        """Gives the message back to a publisher that is a named receiver, unless toSelf is False."""
        receivers = [self.first.uniqueName, self.second.uniqueName]
        self.first.publish_message('listed', receivers=receivers, toSelf=True)
        self.first.publish_message('listedNot', receivers=receivers, toSelf=False)
        self.assertTrue(wait_until(lambda: self.second.has_message('listedNot')))
        self.assertTrue(wait_until(lambda: self.first.has_message('listed')))
        self.assertFalse(self.first.has_message('listedNot'))

    def test_to_self_false(self):
        """Keeps the message away from the publisher when toSelf is False."""
        self.first.publish_message('notme', toSelf=False)
        self.assertTrue(wait_until(lambda: self.second.has_message('notme')))
        holders = self.holders('notme')
        self.assertTrue(holders['server'])
        self.assertFalse(holders['first'])

    def test_to_self_true(self):
        """Keeps the message for the publisher when toSelf is True."""
        self.first.publish_message('me', toSelf=True)
        self.assertTrue(wait_until(lambda: self.first.has_message('me')))

    def test_message_from_a_client_expires_everywhere(self):
        """Removes a message from every locker when its timeout is over."""
        self.first.publish_message('ttl', timeout=1, toSelf=True)
        self.assertTrue(wait_until(lambda: self.second.has_message('ttl')))
        self.assertTrue(wait_until(lambda: not any(self.holders('ttl').values()), seconds=4))

    def test_message_from_the_server_expires_everywhere(self):
        """Removes a message published by the server from every locker when its timeout is over."""
        self.server.publish_message('fromServer', timeout=1)
        self.assertTrue(wait_until(lambda: self.first.has_message('fromServer')))
        self.assertTrue(wait_until(lambda: not any(self.holders('fromServer').values()), seconds=4))

    def test_unique_message_cannot_be_published_twice(self):
        """Raises ValueError when a unique message already exists, and allows a plain republish."""
        self.assertTrue(self.server.publish_message('k', unique=True)[0])
        with self.assertRaises(ValueError):
            self.server.publish_message('k', unique=True)
        self.assertTrue(self.server.publish_message('k')[0])
        self.assertTrue(wait_until(lambda: self.first.has_message('k')))

    def test_pop_message_removes_it(self):
        """Removes a message from the locker that pops it."""
        self.server.publish_message('once')
        self.assertTrue(wait_until(lambda: self.first.has_message('once')))
        self.first.pop_message('once')
        self.assertFalse(self.first.has_message('once'))

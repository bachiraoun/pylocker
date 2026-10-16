"""Tests that a broken publication or order from one client cannot disconnect or confuse the others."""
import time
import unittest
from unittest import mock

from pylocker import ServerLocker

from helpers import FakeServer, LockerTestCase, PASSWORD, signed_order, wait_until
from test_hostile import RawClient, acquire_request


def publication(**changes):
    """Builds a valid publish message from a client and applies changes to it.

    :Parameters:
        #. changes (dict): Fields to set; a value of the string ``<remove>`` removes the field.

    :Returns:
        #. message (dict): The publish message.
    """
    document = {'action': 'publish', 'message': 'hello', 'receivers': None, 'toSelf': False, 'replace': True,
                'timeout': None, 'publication_id': 'pub-1'}
    for key, value in changes.items():
        if value == '<remove>':
            document.pop(key, None)
        else:
            document[key] = value
    return document


BAD_PUBLICATIONS = {
    'no message': publication(message='<remove>'),
    'message is a list': publication(message=['x']),
    'message is a number': publication(message=5),
    'replace is text': publication(replace='yes'),
    'toSelf is text': publication(toSelf='yes'),
    'receivers is text': publication(receivers='everyone'),
    'receivers hold a number': publication(receivers=[5]),
    'timeout is text': publication(timeout='soon'),
    'timeout is negative': publication(timeout=-1),
    'timeout is zero': publication(timeout=0),
    'timeout is a boolean': publication(timeout=True),
    'timeout is not a number': publication(timeout=float('nan')),
    'publication id is a list': publication(publication_id=['x']),
    'publisher name is a list': publication(publisher_unique_name=['x']),
}

BAD_ORDERS = {
    'order is a list': ['stop'],
    'name is a number': {**signed_order(), 'order': 5},
    'proof is a number': {**signed_order(), 'proof': 5},
    'id is missing': {key: value for key, value in signed_order().items() if key != 'order_id'},
    'time is text': {**signed_order(), 'sent_at': 'now'},
    'time is not a number': {**signed_order(), 'sent_at': float('nan')},
    'args is text': {**signed_order(), 'args': 'now'},
    'receivers is text': {**signed_order(), 'receivers': 'everyone'},
}


class TestBrokenPublications(LockerTestCase):
    """Checks what the server does with a publication that is not well formed."""

    def setUp(self):
        """Creates a server, a client to attack and a hand-made client that attacks."""
        super().setUp()
        self.server, self.victim = self.make_pair()
        self.attacker = RawClient(self.server._serverPort, uniqueName='attacker')
        self.addCleanup(self.attacker.close)

    def test_victim_stays_connected_and_stores_nothing(self):
        """Keeps the target connected, and stores nothing, for every kind of broken publication."""
        for label, broken in BAD_PUBLICATIONS.items():
            with self.subTest(label):
                self.attacker.send({**broken, 'receivers': [self.victim.uniqueName]} if label != 'receivers is text'
                                   and label != 'receivers hold a number' else broken)
                time.sleep(0.15)
                self.assertTrue(self.victim.isClient)
                self.assertEqual(self.victim._publications, {})
                self.assertEqual(self.server._publications, {})

    def test_server_does_not_pass_a_broken_publication_on(self):
        """Never hands a broken publication to the receiver, so the check does not depend on the receiver."""
        with mock.patch.object(self.victim, '_store_publication') as stored:
            for label, broken in BAD_PUBLICATIONS.items():
                if 'receivers' not in label:
                    self.attacker.send({**broken, 'receivers': [self.victim.uniqueName]})
            self.attacker.send(publication(message='fine', receivers=[self.victim.uniqueName]))
            self.assertTrue(wait_until(lambda: stored.call_count >= 1))
            time.sleep(0.3)
        self.assertEqual(stored.call_count, 1)
        self.assertEqual(stored.call_args[0][0]['message'], 'fine')

    def test_valid_publication_still_arrives(self):
        """Delivers a correct publication sent by hand, after the broken ones."""
        for broken in BAD_PUBLICATIONS.values():
            self.attacker.send({**broken, 'receivers': [self.victim.uniqueName]})
        self.attacker.send(publication(message='alive', receivers=[self.victim.uniqueName], timeout=30))
        self.assertTrue(wait_until(lambda: self.victim.has_message('alive')))

    def test_publisher_name_cannot_be_forged(self):
        """Shows the receiver the real name of the sender, whatever the message says."""
        self.attacker.send(publication(message='forged', receivers=[self.victim.uniqueName],
                                       client_unique_name='someone-else', publisher_unique_name='someone-else'))
        self.assertTrue(wait_until(lambda: self.victim.has_message('forged')))
        stored = self.victim._publications['forged'][0]
        self.assertEqual(stored['publisher_unique_name'], 'attacker')

    def test_lock_owner_name_cannot_be_forged(self):
        """Records the real name of the sender in a lock, whatever the request says."""
        path = self.lock_path('who')
        self.attacker.send(acquire_request(path=[path], client_unique_name='someone-else'))
        self.assertEqual(self.attacker.read()['action'], 'acquired')
        self.assertEqual(self.server._pathsLUT[path.replace('\\', '/')]['client_unique_name'], 'attacker')


class TestBrokenOrders(LockerTestCase):
    """Checks what the server does with an order that is not well formed."""

    def test_broken_orders_are_not_passed_on(self):
        """Hands the receiver only the order that is well formed."""
        server, victim = self.make_pair()
        attacker = RawClient(server._serverPort, uniqueName='attacker')
        self.addCleanup(attacker.close)
        with mock.patch.object(victim, '_handle_order') as handled:
            for label, broken in BAD_ORDERS.items():
                if isinstance(broken, dict):
                    attacker.send({**broken, 'action': 'order'})
            attacker.send(signed_order(orderName='dance', args=()))
            self.assertTrue(wait_until(lambda: handled.call_count >= 1))
            time.sleep(0.3)
        self.assertEqual(handled.call_count, 1)
        self.assertTrue(victim.isClient and server.isServer)

    def test_message_that_is_not_an_object_only_ends_the_sender(self):
        """Ends the connection of a client that sends a list, and leaves the others connected."""
        server, victim = self.make_pair()
        attacker = RawClient(server._serverPort, uniqueName='attacker')
        self.addCleanup(attacker.close)
        attacker.connection.sendall(b'[1, 2, 3]\n')
        self.assertTrue(wait_until(lambda: 'attacker' not in server._clientsLUT))
        self.assertTrue(victim.isClient)


class TestClientKeepsReading(LockerTestCase):
    """Checks that a client survives messages from the server that it cannot handle."""

    def setUp(self):
        """Connects a client to a fake server."""
        super().setUp()
        self.fake = FakeServer()
        self.addCleanup(self.fake.close)
        self.client = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False, allowServing=False,
                                              connectTimeout=3))
        self.client.start(address='127.0.0.1', port=self.fake.port, connectAttempts=1)
        self.assertTrue(self.client.isClient)

    def assert_client_alive(self, word):
        """Fails unless the client is connected and receives a correct publication.

        :Parameters:
            #. word (str): The text to publish.
        """
        self.fake.send({'action': 'publish', 'message': word, 'replace': True, 'publisher_unique_name': 'fake-id',
                        'publication_id': word})
        self.assertTrue(wait_until(lambda: self.client.has_message(word)))
        self.assertTrue(self.client.isClient)

    def test_broken_publications_are_ignored(self):
        """Ignores a broken publication and goes on reading."""
        for label, broken in BAD_PUBLICATIONS.items():
            with self.subTest(label):
                self.fake.send(broken)
        self.assert_client_alive('after publications')
        self.assertEqual(list(self.client._publications), ['after publications'])

    def test_messages_that_fail_in_a_handler_are_ignored(self):
        """Goes on reading after a message that makes its handler raise."""
        self.fake.send({'action': 'acquired', 'request_unique_id': ['not', 'hashable']})
        self.fake.send({'action': 'exceeded_maximum_lock_time', 'request_unique_id': ['not', 'hashable']})
        self.fake.send_raw(b'[1, 2, 3]\n')
        self.fake.send_raw(b'"just text"\n')
        self.assert_client_alive('after handlers')

    def test_handler_that_raises_does_not_end_the_connection(self):
        """Goes on reading after the handler of a message raised."""
        with mock.patch.object(self.client, '_handle_order', side_effect=RuntimeError('boom')):
            self.fake.send(signed_order())
            time.sleep(0.4)
        self.assertTrue(self.client.isClient)
        self.assert_client_alive('after the error')

    def test_store_publication_ignores_a_broken_publication(self):
        """Returns without raising and stores nothing for a publication that is not well formed."""
        locker = self.track(ServerLocker(PASSWORD, serverFile=False, autoconnect=False))
        for broken in BAD_PUBLICATIONS.values():
            locker._store_publication(broken)
        self.assertEqual(locker._publications, {})


class TestPublishArguments(LockerTestCase):
    """Checks the arguments of publish_message."""

    def test_wrong_types_raise_type_error(self):
        """Raises TypeError for flags that are not boolean and receivers that are not names."""
        server, client = self.make_pair()
        for kwargs in ({'replace': 'yes'}, {'toSelf': 1}, {'unique': None}, {'receivers': [5]}):
            with self.subTest(kwargs):
                with self.assertRaises(TypeError):
                    client.publish_message('word', **kwargs)
        self.assertTrue(client.publish_message('word', receivers=[server.uniqueName], replace=False)[0])


class TestValidators(unittest.TestCase):
    """Checks the rules that decide whether a publication or an order is well formed."""

    def test_correct_messages_have_no_problem(self):
        """Returns None for correct publications and orders."""
        for document in (publication(), publication(timeout=2.5, receivers=['a', 'b']),
                         publication(receivers='<remove>', toSelf='<remove>', replace='<remove>', timeout='<remove>')):
            self.assertIsNone(ServerLocker._publication_problem(document))
        self.assertIsNone(ServerLocker._order_message_problem(signed_order()))
        self.assertIsNone(ServerLocker._order_message_problem({**signed_order(), 'args': None, 'receivers': ['a']}))

    def test_problems_name_the_field(self):
        """Names the field that is wrong."""
        self.assertIn('message', ServerLocker._publication_problem(publication(message=1)))
        self.assertIn('timeout', ServerLocker._publication_problem(publication(timeout=-3)))
        self.assertIn('sent_at', ServerLocker._order_message_problem({**signed_order(), 'sent_at': None}))


if __name__ == '__main__':
    unittest.main()

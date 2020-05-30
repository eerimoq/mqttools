import ssl
import logging
import asyncio
import unittest

import mqttools


class BrokerTest(unittest.TestCase):

    def create_broker(self):
        broker = mqttools.Broker(('localhost', 0))

        async def broker_wrapper():
            with self.assertRaises(asyncio.CancelledError):
                await broker.serve_forever()

        return broker, asyncio.create_task(broker_wrapper())

    def test_multiple_subscribers(self):
        asyncio.run(self.multiple_subscribers())

    async def multiple_subscribers(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup subscriber 1.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup subscriber 2.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su2'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer_2.write(subscribe)
            suback = await reader_2.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_3, writer_3 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_3.write(connect)
            connack = await reader_3.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish a topic.
            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer_3.write(publish)

            # Receive the publish in both subscribers.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/b\x00apa')
            publish = await reader_2.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/b\x00apa')

            # Cleanly disconnect subscriber 1.
            disconnect = b'\xe0\x02\x00\x00'
            writer_1.write(disconnect)
            writer_1.close()

            # Publish another topic, now only to subscriber 2.
            publish = b'\x30\x0a\x00\x04/a/b\x00boo'
            writer_3.write(publish)

            # Receive the publish in subscriber 2.
            publish = await reader_2.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/b\x00boo')

            writer_2.close()
            writer_3.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_subscribe_to_same_topic_twice(self):
        asyncio.run(self.subscribe_to_same_topic_twice())

    async def subscribe_to_same_topic_twice(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Subscribe to "/a/b" and "/b/#" twice, all should be
            # successful. Subscribe to "/d/e" once.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            for _ in range(2):
                subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
                writer_1.write(subscribe)
                suback = await reader_1.readexactly(6)
                self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            for _ in range(2):
                subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/b/#\x00'
                writer_1.write(subscribe)
                suback = await reader_1.readexactly(6)
                self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/d/e\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish "/a/b", "/b/c" and "/d/e" once. They should all
            # be received once.
            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/b/c\x00cow'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/d/e\x00123'
            writer_2.write(publish)

            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/b\x00apa')
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/b/c\x00cow')
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/d/e\x00123')

            # Unsubscribe from "/a/b" and "/b/#" and publish
            # again. Only "/d/e" should be received.
            unsubscribe = b'\xa2\x0f\x00\x02\x00\x00\x04/a/b\x00\x04/b/#'
            writer_1.write(unsubscribe)
            unsuback = await reader_1.readexactly(7)
            self.assertEqual(unsuback, b'\xb0\x05\x00\x02\x00\x00\x00')

            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/b/c\x00cow'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/d/e\x00123'
            writer_2.write(publish)

            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/d/e\x00123')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_unsubscribe(self):
        asyncio.run(self.unsubscribe())

    async def unsubscribe(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber. Subscribe to /a/a, /a/b, /a/c and /a/d.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = (
                b'\x82\x1f\x00\x01\x00\x00\x04/a/a\x00\x00\x04/a/b\x00\x00\x04'
                b'/a/c\x00\x00\x04/a/d\x00')
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(9)
            self.assertEqual(suback, b'\x90\x07\x00\x01\x00\x00\x00\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish /a/a and /a/c.
            publish = b'\x30\x0a\x00\x04/a/a\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/a/c\x00cow'
            writer_2.write(publish)

            # Receive /a/a and /a/c in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/a\x00apa')
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/c\x00cow')

            # Unsubscribe from /a/a and /a/c.
            unsubscribe = b'\xa2\x0f\x00\x02\x00\x00\x04/a/a\x00\x04/a/c'
            writer_1.write(unsubscribe)
            unsuback = await reader_1.readexactly(7)
            self.assertEqual(unsuback, b'\xb0\x05\x00\x02\x00\x00\x00')

            # Publish /a/a, /a/c and then /a/b, /a/a should not be
            # received by the subscriber.
            publish = b'\x30\x0a\x00\x04/a/a\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/a/c\x00cow'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/a/b\x00mas'
            writer_2.write(publish)

            # Receive /a/b in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/b\x00mas')

            # Unsubscribe from /a/b.
            unsubscribe = b'\xa2\x09\x00\x02\x00\x00\x04/a/b'
            writer_1.write(unsubscribe)
            unsuback = await reader_1.readexactly(6)
            self.assertEqual(unsuback, b'\xb0\x04\x00\x02\x00\x00')

            # Unsubscribe from /a/b again should fail.
            unsubscribe = b'\xa2\x09\x00\x02\x00\x00\x04/a/b'
            writer_1.write(unsubscribe)
            unsuback = await reader_1.readexactly(6)
            self.assertEqual(unsuback, b'\xb0\x04\x00\x02\x00\x11')

            # Unsubscribe from /a/# is an invalid topic in the current
            # implementation.
            unsubscribe = b'\xa2\x09\x00\x02\x00\x00\x04/a/#'
            writer_1.write(unsubscribe)
            unsuback = await reader_1.readexactly(6)
            self.assertEqual(unsuback, b'\xb0\x04\x00\x02\x00\x11')

            # Publish /a/b and then /a/d, /a/b should not be received
            # by the subscriber.
            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/a/d\x00mas'
            writer_2.write(publish)

            # Receive /a/d in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/d\x00mas')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_multi_level_wildcard_tennis(self):
        asyncio.run(self.multi_level_wildcard_tennis())

    async def multi_level_wildcard_tennis(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber and subscribe to
            # "sport/tennis/player1/#".
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x1c\x00\x01\x00\x00\x16sport/tennis/player1/#\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish "sport/tennis/player1".
            publish = b'\x30\x1a\x00\x14sport/tennis/player1\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(28)
            self.assertEqual(
                publish,
                b'\x30\x1a\x00\x14sport/tennis/player1\x00apa')

            # Publish "sport/tennis/player1/ranking".
            publish = b'\x30\x22\x00\x1csport/tennis/player1/ranking\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(36)
            self.assertEqual(
                publish,
                b'\x30\x22\x00\x1csport/tennis/player1/ranking\x00apa')

            # Publish "sport/tennis/player1/ranking/wimbledon".
            publish = (
                b'\x30\x2c\x00\x26sport/tennis/player1/ranking/wimbledon\x00apa')
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(46)
            self.assertEqual(
                publish,
                b'\x30\x2c\x00\x26sport/tennis/player1/ranking/wimbledon\x00apa')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_multi_level_wildcard_any(self):
        asyncio.run(self.multi_level_wildcard_any())

    async def multi_level_wildcard_any(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber and subscribe to "#".
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x07\x00\x01\x00\x00\x01#\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish "sport/tennis/player1".
            publish = b'\x30\x1a\x00\x14sport/tennis/player1\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(28)
            self.assertEqual(
                publish,
                b'\x30\x1a\x00\x14sport/tennis/player1\x00apa')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_single_level_wildcard_only(self):
        asyncio.run(self.single_level_wildcard_only())

    async def single_level_wildcard_only(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber and subscribe to "+".
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x07\x00\x01\x00\x00\x01+\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish "sport".
            publish = b'\x30\x0b\x00\x05sport\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(13)
            self.assertEqual(
                publish,
                b'\x30\x0b\x00\x05sport\x00apa')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_single_level_wildcard_middle(self):
        asyncio.run(self.single_level_wildcard_middle())

    async def single_level_wildcard_middle(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber and subscribe to "sport/+/player1".
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x15\x00\x01\x00\x00\x0fsport/+/player1\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish "sport/tennis/player1".
            publish = b'\x30\x1a\x00\x14sport/tennis/player1\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(28)
            self.assertEqual(
                publish,
                b'\x30\x1a\x00\x14sport/tennis/player1\x00apa')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_single_level_wildcard_end(self):
        asyncio.run(self.single_level_wildcard_end())

    async def single_level_wildcard_end(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber and subscribe to "sport/tennis/+".
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x14\x00\x01\x00\x00\x0esport/tennis/+\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish "sport/tennis/player1/ranking". It should not be
            # received.
            publish = b'\x30\x22\x00\x1csport/tennis/player1/ranking\x00apa'
            writer_2.write(publish)

            # Publish "sport/tennis/player1".
            publish = b'\x30\x1a\x00\x14sport/tennis/player1\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(28)
            self.assertEqual(
                publish,
                b'\x30\x1a\x00\x14sport/tennis/player1\x00apa')

            # Publish "sport/tennis/player2".
            publish = b'\x30\x1a\x00\x14sport/tennis/player2\x00apa'
            writer_2.write(publish)

            # Receive the publish.
            publish = await reader_1.readexactly(28)
            self.assertEqual(
                publish,
                b'\x30\x1a\x00\x14sport/tennis/player2\x00apa')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_unsubscribe_wildcard(self):
        asyncio.run(self.unsubscribe_wildcard())

    async def unsubscribe_wildcard(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber. Subscribe to a/# and b/+.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = (
                b'\x82\x11\x00\x01\x00\x00\x04/a/#\x00\x00\x04/b/+\x00')
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(7)
            self.assertEqual(suback, b'\x90\x05\x00\x01\x00\x00\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish /a/a and /b/c.
            publish = b'\x30\x0a\x00\x04/a/a\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/b/c\x00cow'
            writer_2.write(publish)

            # Receive /a/a and /b/c in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/a\x00apa')
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/b/c\x00cow')

            # Unsubscribe from /a/#.
            unsubscribe = b'\xa2\x09\x00\x02\x00\x00\x04/a/#'
            writer_1.write(unsubscribe)
            unsuback = await reader_1.readexactly(6)
            self.assertEqual(unsuback, b'\xb0\x04\x00\x02\x00\x00')

            # Publish /a/a and then /b/c, /a/a should not be received
            # by the subscriber.
            publish = b'\x30\x0a\x00\x04/a/a\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/b/c\x00cow'
            writer_2.write(publish)

            # Receive /b/c in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/b/c\x00cow')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_resume_session(self):
        asyncio.run(self.resume_session())

    async def resume_session(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Try to resume a session that does not exist. A new
            # session should be created and immediately removed when
            # the connetion ends as the expiry interval is 0 (not
            # given).
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = (b'\x10\x10\x00\x04MQTT\x05\x00\x00\x00\x00\x00\x03res')
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')
            writer_1.close()

            # Try to resume a session that does not exist. A new
            # session should be created.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = (b'\x10\x15\x00\x04MQTT\x05\x00\x00\x00\x05\x11\xff\xff'
                       b'\xff\xff\x00\x03res')
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')
            writer_1.close()

            # Resume the session.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = (b'\x10\x15\x00\x04MQTT\x05\x00\x00\x00\x05\x11\xff\xff'
                       b'\xff\xff\x00\x03res')
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x01\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Setup a publisher.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish /a/b.
            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer_2.write(publish)

            # Receive /a/b in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/b\x00apa')

            # Perform a clean start and subscribe to /a/c.
            writer_1.close()
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03res'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/c\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Publish /a/b and then /a/c.
            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer_2.write(publish)
            publish = b'\x30\x0a\x00\x04/a/c\x00apa'
            writer_2.write(publish)

            # Receive /a/c in the subscriber.
            publish = await reader_1.readexactly(12)
            self.assertEqual(publish, b'\x30\x0a\x00\x04/a/c\x00apa')

            writer_1.close()
            writer_2.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_ping(self):
        asyncio.run(self.ping())

    async def ping(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the pinger.
            reader, writer = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer.write(connect)
            connack = await reader.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            pingreq = b'\xc0\x00'
            writer.write(pingreq)
            pingresp = await reader.readexactly(2)
            self.assertEqual(pingresp, b'\xd0\x00')

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_authentication_with_user_name_and_password(self):
        asyncio.run(self.authentication_with_user_name_and_password())

    async def authentication_with_user_name_and_password(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Connect with user name and password, and get 0x86 in
            # response.
            reader, writer = await asyncio.open_connection(*address)
            connect = (
                b'\x10\x1c\x00\x04MQTT\x05\xc0\x00\x00\x00\x00\x03su1\x00\x04user'
                b'\x00\x04pass')
            writer.write(connect)
            connack = await reader.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x86\x06\x24\x00\x28\x00\x2a\x00')

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_authentication_method(self):
        asyncio.run(self.authentication_method())

    async def authentication_method(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Connect with authentication method, and get 0x8c in
            # response.
            reader, writer = await asyncio.open_connection(*address)
            connect = (
                b'\x10\x1b\x00\x04MQTT\x05\x00\x00\x00\x0b\x15\x00\x08bad-auth'
                b'\x00\x03su1')
            writer.write(connect)
            connack = await reader.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x8c\x06\x24\x00\x28\x00\x2a\x00')

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_missing_connect_packet(self):
        asyncio.run(self.missing_connect_packet())

    async def missing_connect_packet(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Send a publish packet instead of connect, nothing should
            # be received in response.
            reader, writer = await asyncio.open_connection(*address)
            publish = b'\x30\x0a\x00\x04/a/b\x00apa'
            writer.write(publish)

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader.read(1), 0.3)

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_malformed_packet(self):
        asyncio.run(self.malformed_packet())

    async def malformed_packet(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Malformed publish with property 0 should result in a
            # disconnect by the broker.
            reader, writer = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03mal'
            writer.write(connect)
            connack = await reader.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            publish = b'\x30\x0b\x01\x00\x04/a/b\x00apa'
            writer.write(publish)
            disconnect = await reader.readexactly(4)
            self.assertEqual(disconnect, b'\xe0\x02\x81\x00')

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_protocol_error(self):
        asyncio.run(self.protocol_error())

    async def protocol_error(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # It's a protocol error sending ping response to the
            # broker. should result in a disconnect by the broker.
            reader, writer = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03mal'
            writer.write(connect)
            connack = await reader.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            pingresp = b'\xd0\x00'
            writer.write(pingresp)
            disconnect = await reader.readexactly(4)
            self.assertEqual(disconnect, b'\xe0\x02\x82\x00')

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_maximum_packet_size(self):
        asyncio.run(self.maximum_packet_size())

    async def maximum_packet_size(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Connect with maximum packet size set to 50.
            reader, writer = await asyncio.open_connection(*address)
            connect = (
                b'\x10\x15\x00\x04MQTT\x05\x00\x00\x00\x05\x27\x00\x00\x00\x32'
                b'\x00\x03su1')
            writer.write(connect)
            connack = await reader.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Subscribe to a topic.
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer.write(subscribe)
            suback = await reader.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Publish a message of 51 bytes on the subscribed topic,
            # and then one of 50 bytes.
            publish = (
                b'\x30\x31\x00\x04/a/b\x001234567890012345678900123456789001234'
                b'56789')
            self.assertEqual(len(publish), 51)
            writer.write(publish)
            publish = (
                b'\x30\x30\x00\x04/a/b\x001234567890012345678900123456789001234'
                b'5678')
            self.assertEqual(len(publish), 50)
            writer.write(publish)

            # Only the short packet is received.
            publish = await reader.readexactly(50)
            self.assertEqual(
                publish,
                b'\x30\x30\x00\x04/a/b\x001234567890012345678900123456789001234'
                b'5678')

            writer.close()
            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_publish_will(self):
        asyncio.run(self.publish_will())

    async def publish_will(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber. Subscribe to 'foo' and 'a/#'.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x09\x00\x01\x00\x00\x03foo\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')
            subscribe = b'\x82\x09\x00\x01\x00\x00\x03a/#\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Connect with will topic 'foo', message 'bar' and retain
            # False.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = (
                b'\x10\x1a\x00\x04MQTT\x05\x06\x00\x00\x00\x00\x02id\x00\x00'
                b'\x03foo\x00\x03bar')
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Connect with will topic 'a/b', message 'fie' and retain
            # True.
            reader_3, writer_3 = await asyncio.open_connection(*address)
            connect = (
                b'\x10\x1a\x00\x04MQTT\x05\x26\x00\x00\x00\x00\x02ko\x00\x00'
                b'\x03a/b\x00\x03fie')
            writer_3.write(connect)
            connack = await reader_3.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Verify that the will is published when the client is
            # lost.
            writer_2.close()
            publish = await reader_1.readexactly(11)
            self.assertEqual(
                publish,
                b'\x30\x09\x00\x03foo\x00bar')

            # Verify that the will is published when the second client
            # is lost.
            writer_3.close()
            publish = await reader_1.readexactly(11)
            self.assertEqual(
                publish,
                b'\x30\x09\x00\x03a/b\x00fie')

            writer_1.close()

            # Setup the subscriber again with the same subscriptions
            # 'foo' and 'a/#'. Only the retained will a/b should be
            # received.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x09\x00\x01\x00\x00\x03foo\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')
            subscribe = b'\x82\x09\x00\x01\x00\x00\x03a/#\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')
            publish = await reader_1.readexactly(11)
            self.assertEqual(
                publish,
                b'\x30\x09\x00\x03a/b\x00fie')

            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_publish_retained_on_subscribe(self):
        asyncio.run(self.publish_retained_on_subscribe())

    async def publish_retained_on_subscribe(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup a publisher.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03pub'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Publish a topic with retain set.
            publish = b'\x31\x0a\x00\x04/a/b\x00apa'
            writer_1.write(publish)

            # Setup the subscriber.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer_2.write(subscribe)
            suback = await reader_2.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Receive the retained message.
            publish = await reader_2.readexactly(12)
            self.assertEqual(
                publish,
                b'\x30\x0a\x00\x04/a/b\x00apa')

            writer_2.close()

            # Publish the same topic with retain set with no data to
            # remove the retained message from the broker..
            publish = b'\x31\x07\x00\x04/a/b\x00'
            writer_1.write(publish)

            # Setup the subscriber again.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x0a\x00\x01\x00\x00\x04/a/b\x00'
            writer_2.write(subscribe)
            suback = await reader_2.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # No retained message should be received.
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader_2.readexactly(1), 0.250)

            writer_2.close()

            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_do_not_publish_will_on_normal_disconnect(self):
        asyncio.run(self.do_not_publish_will_on_normal_disconnect())

    async def do_not_publish_will_on_normal_disconnect(self):
        broker, broker_task = self.create_broker()

        async def tester():
            address = await broker.getsockname()

            # Setup the subscriber. Subscribe to 'foo'.
            reader_1, writer_1 = await asyncio.open_connection(*address)
            connect = b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03su1'
            writer_1.write(connect)
            connack = await reader_1.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')
            subscribe = b'\x82\x09\x00\x01\x00\x00\x03foo\x00'
            writer_1.write(subscribe)
            suback = await reader_1.readexactly(6)
            self.assertEqual(suback, b'\x90\x04\x00\x01\x00\x00')

            # Connect with will topic 'foo' and message 'bar'.
            reader_2, writer_2 = await asyncio.open_connection(*address)
            connect = (
                b'\x10\x1a\x00\x04MQTT\x05\x06\x00\x00\x00\x00\x02id\x00\x00'
                b'\x03foo\x00\x03bar')
            writer_2.write(connect)
            connack = await reader_2.readexactly(11)
            self.assertEqual(
                connack,
                b'\x20\x09\x00\x00\x06\x24\x00\x28\x00\x2a\x00')

            # Verify that the will is not published when the client is
            # normally disconnected.
            disconnect = b'\xe0\x02\x00\x00'
            writer_2.write(disconnect)
            writer_2.close()

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader_1.readexactly(1), 0.250)

            writer_1.close()

            broker_task.cancel()

        await asyncio.wait_for(asyncio.gather(broker_task, tester()), 1)

    def test_create_broker_addresses(self):
        asyncio.run(self.create_broker_addresses())

    async def create_broker_addresses(self):
        mqttools.Broker('localhost')
        mqttools.Broker(('localhost', 5))
        mqttools.Broker(('localhost', 10, ssl.create_default_context()))
        mqttools.Broker([
            ('localhost', 5),
            ('localhost', 10, ssl.create_default_context())
        ])


logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()

import asyncio
import unittest

import mqttools
from mqttools.common import PropertyIds

from .utils import get_broker_address


class RequestResponseTest(unittest.TestCase):

    def create_broker(self):
        broker = mqttools.Broker(("localhost", 0))

        async def broker_wrapper():
            with self.assertRaises(asyncio.CancelledError):
                await broker.serve_forever()

        return broker, asyncio.create_task(broker_wrapper())

    def test_request_response(self):
        asyncio.run(self.request_response())

    async def request_response(self):
        broker, broker_task = self.create_broker()

        host, port = await get_broker_address(broker)

        request_topic = "/a"
        response_topic = "/b"
        request = b"ping"
        response = b"pong"

        requester = mqttools.Client(host, port)
        await requester.start()
        await requester.subscribe(response_topic)

        responder = mqttools.Client(host, port)
        await responder.start()
        await responder.subscribe(request_topic)

        requester.publish(request_topic,
                          request,
                          retain=True,
                          properties={PropertyIds.RESPONSE_TOPIC: response_topic})

        topic, _, properties = await responder.messages.get()
        self.assertEqual(topic, request_topic)
        responder.publish(properties[PropertyIds.RESPONSE_TOPIC], response)

        topic, message, _ = await requester.messages.get()
        self.assertEqual(topic, response_topic)
        self.assertEqual(message, response)

        broker_task.cancel()
        await asyncio.wait_for(broker_task, 1)

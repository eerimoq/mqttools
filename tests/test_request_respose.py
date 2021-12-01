import asyncio
import unittest

import mqttools

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

        requester.publish(mqttools.Message(request_topic,
                                           request,
                                           True,
                                           response_topic))

        message = await responder.messages.get()
        self.assertEqual(message.topic, request_topic)
        responder.publish(mqttools.Message(message.response_topic, response))

        message = await requester.messages.get()
        self.assertEqual(message.topic, response_topic)
        self.assertEqual(message.message, response)

        broker_task.cancel()
        await asyncio.wait_for(broker_task, 1)

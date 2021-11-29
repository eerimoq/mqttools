import asyncio
import logging
import unittest

import mqttools
from mqttools.common import PropertyIds


logger = logging.getLogger(__name__)


class ReqRepTest(unittest.TestCase):
    def create_broker(self):
        broker = mqttools.Broker(("localhost", 0))

        async def broker_wrapper():
            with self.assertRaises(asyncio.CancelledError):
                await broker.serve_forever()

        return broker, asyncio.create_task(broker_wrapper())

    def test_reqrep_timeout(self):
        asyncio.run(self.reqrep())

    async def reqrep(self):
        broker, broker_task = self.create_broker()
        address = await broker.getsockname()
        host, port = address[:2]

        request_topic = "/a"
        response_topic = "/b"
        request = b"ping"
        response = b"pong"
        repetitions = 3

        async def requester():
            req = mqttools.Client(host, port)
            await req.start()
            await req.subscribe(response_topic)
            for _ in range(repetitions):
                req.publish(
                    request_topic,
                    request,
                    retain=True,
                    properties={PropertyIds.RESPONSE_TOPIC: response_topic},
                )
                topic, message, properties = await req.messages.get()
                logger.debug(f"Received on {topic}: {message}")
                self.assertEqual(message, response)

        async def responder():
            rep = mqttools.Client(host, port)
            await rep.start()
            await rep.subscribe(request_topic)
            while True:
                topic, message, properties = await rep.messages.get()
                logger.debug(f"Received on {topic}: {message} with {properties}")
                rep.publish(properties.get(PropertyIds.RESPONSE_TOPIC), response)

        with self.assertRaises(asyncio.exceptions.TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(broker_task, responder(), requester()), timeout=1
            )


logging.basicConfig(level=logging.DEBUG)


if __name__ == "__main__":
    unittest.main()

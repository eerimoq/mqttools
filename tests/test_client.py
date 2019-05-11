import logging
import asyncio
import unittest

import mqttools


async def broker_main(listener):
    async with listener:
        try:
            await listener.serve_forever()
        except asyncio.CancelledError:
            pass


class ClientTest(unittest.TestCase):

    def test_connack_timeout(self):
        asyncio.run(self.connack_timeout())

    async def connack_timeout(self):
        def on_client_connected(reader, writer):
            pass

        listener = await asyncio.start_server(on_client_connected, 'localhost', 0)

        async def client_main():
            client = mqttools.Client(*listener.sockets[0].getsockname(),
                                     'connack',
                                     response_timeout=0.1)

            with self.assertRaises(mqttools.TimeoutError):
                await client.start()

            listener.close()

        await asyncio.wait_for(
            asyncio.gather(broker_main(listener), client_main()), 1)

    def test_subscribe_timeout(self):
        asyncio.run(self.subscribe_timeout())

    async def subscribe_timeout(self):
        def on_client_connected(reader, writer):
            # CONNACK
            writer.write(b'\x20\x03\x00\x00\x00')

        listener = await asyncio.start_server(on_client_connected, 'localhost', 0)

        async def client_main():
            client = mqttools.Client(*listener.sockets[0].getsockname(),
                                     'suback',
                                     response_timeout=0.1)
            await client.start()

            with self.assertRaises(mqttools.TimeoutError):
                await client.subscribe('/foo')

            listener.close()

        await asyncio.wait_for(
            asyncio.gather(broker_main(listener), client_main()), 1)

    def test_unsubscribe_timeout(self):
        asyncio.run(self.unsubscribe_timeout())

    async def unsubscribe_timeout(self):
        def on_client_connected(reader, writer):
            # CONNACK
            writer.write(b'\x20\x03\x00\x00\x00')

        listener = await asyncio.start_server(on_client_connected, 'localhost', 0)

        async def client_main():
            client = mqttools.Client(*listener.sockets[0].getsockname(),
                                     'unsuback',
                                     response_timeout=0.1)
            await client.start()

            with self.assertRaises(mqttools.TimeoutError):
                await client.unsubscribe('/foo')

            listener.close()

        await asyncio.wait_for(
            asyncio.gather(broker_main(listener), client_main()), 1)

    def test_client_id(self):
        client = mqttools.Client('localhost', 0)
        self.assertEqual(client.client_id[:9], 'mqttools-')



logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()

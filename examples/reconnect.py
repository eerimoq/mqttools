import asyncio
import logging

import mqttools


async def handle_messages(client):
    while True:
        topic, message = await client.messages.get()
        print(f'Got {message} on {topic}.')

        if topic is None:
            print('Connection lost.')
            break


async def reconnector():
    client = mqttools.Client('localhost',
                             1883,
                             subscriptions=['foobar'],
                             connect_delays=[1, 2, 4, 8])

    while True:
        await client.start()
        await handle_messages(client)
        await client.stop()


logging.basicConfig(level=logging.INFO)

asyncio.run(reconnector())

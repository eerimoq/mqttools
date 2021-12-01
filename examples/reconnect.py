import asyncio

import mqttools


async def handle_messages(client):
    while True:
        message = await client.messages.get()

        if message is None:
            print('Connection lost.')
            break

        print(f'Got {message.message} on {message.topic}.')


async def reconnector():
    client = mqttools.Client('localhost',
                             1883,
                             subscriptions=['foobar'],
                             connect_delays=[1, 2, 4, 8])

    while True:
        await client.start()
        await handle_messages(client)
        await client.stop()

asyncio.run(reconnector())

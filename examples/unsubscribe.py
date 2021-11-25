import asyncio

import mqttools


async def unsubscriber():
    client = mqttools.Client('localhost',
                             1883,
                             keep_alive_s=5)

    await client.start()

    print('Subscribing to /test/mqttools/foo.')
    await client.subscribe('/test/mqttools/foo')
    topic, message = await client.messages.get()

    print(f'Topic:   {topic}')
    print(f'Message: {message}')

    print('Unsubscribing from /test/mqttools/foo.')
    await client.unsubscribe('/test/mqttools/foo')

    # Should only return when the broker connection is lost.
    topic, message = await client.messages.get()

    print(f'Topic:   {topic}')
    print(f'Message: {message}')


asyncio.run(unsubscriber())

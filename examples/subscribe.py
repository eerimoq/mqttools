import asyncio

import mqttools


async def subscriber():
    client = mqttools.Client('localhost', 1883)

    await client.start()

    # Subscribe to two topics in parallel.
    await asyncio.gather(
        client.subscribe('$SYS/#'),
        client.subscribe('/test/mqttools/foo')
    )

    print('Waiting for messages.')

    while True:
        message = await client.messages.get()

        if message is None:
            print('Broker connection lost!')
            break

        print(f'Topic:   {message.topic}')
        print(f'Message: {message.message}')


asyncio.run(subscriber())

import asyncio

import mqttools


async def publish_to_self():
    client = mqttools.Client('localhost', 1883)

    await client.start()
    await client.subscribe('/test/mqttools/foo')

    client.publish(mqttools.Message('/test/mqttools/foo', b'publish_to_self message'))
    message = await client.messages.get()

    if message is None:
        print('Broker connection lost!')
    else:
        print(f'Topic:   {message.topic}')
        print(f'Message: {message.message}')


asyncio.run(publish_to_self())

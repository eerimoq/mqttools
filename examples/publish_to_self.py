import asyncio
import mqttools


async def publish_to_self():
    client = mqttools.Client('broker.hivemq.com', 1883)

    await client.start()
    await client.subscribe('/test/mqttools/foo')

    client.publish('/test/mqttools/foo', b'publish_to_self message')
    topic, message = await client.messages.get()

    if topic is None:
        print('Broker connection lost!')
    else:
        print(f'Topic:   {topic}')
        print(f'Message: {message}')


asyncio.run(publish_to_self())

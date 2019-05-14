import asyncio
import mqttools


async def subscriber():
    client = mqttools.Client('broker.hivemq.com', 1883)

    await client.start()

    # Subscribe to two topics in parallel.
    await asyncio.gather(
        client.subscribe('$SYS/#'),
        client.subscribe('/test/mqttools/foo')
    )

    print('Waiting for messages.')

    while True:
        topic, message = await client.messages.get()

        if topic is None:
            print('Broker connection lost!')
            break

        print(f'Topic:   {topic}')
        print(f'Message: {message}')


asyncio.run(subscriber())

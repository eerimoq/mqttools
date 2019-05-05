import asyncio
import mqttools

async def subscriber():
    client = mqttools.Client('broker.hivemq.com', 1883)

    await client.start()

    # Subscribe to three topics in parallel.
    await asyncio.gather(
        client.subscribe('$SYS/broker/uptime', 0),
        client.subscribe('$SYS/broker/bytes/sent', 0),
        client.subscribe('/test/mqttools/foo', 0)
    )

    while True:
        topic, message = await client.messages.get()

        print(f'Topic:   {topic}')
        print(f'Message: {message}')

asyncio.run(subscriber())

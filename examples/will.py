import asyncio
import mqttools

async def will():
    client = mqttools.Client('broker.hivemq.com',
                             1883,
                             'mqttools-will',
                             '/my/will/topic',
                             b'my-will-message',
                             0)

    await client.start()
    await client.stop()
    print("Successfully connected with will.")

asyncio.run(will())

import asyncio
import mqttools

async def will():
    client = mqttools.Client('test.mosquitto.org',
                             1883,
                             'mqttools-will',
                             '/my/will/topic',
                             b'my-will-message',
                             0)

    await client.start()
    await client.stop()
    print("Successfully connected with will.")

asyncio.run(will())

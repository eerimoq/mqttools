import asyncio
import mqttools

async def will():
    client = mqttools.Client('test.mosquitto.org',
                             1883,
                             b'mqttools-will',
                             b'/my/will/topic',
                             b'my-will-message',
                             0)

    await client.start()
    await client.stop()

asyncio.run(will())

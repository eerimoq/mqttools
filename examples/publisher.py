import asyncio
import mqttools

async def publisher():
    client = mqttools.Client('test.mosquitto.org', 1883, b'mqttools-publish')

    await client.start()
    await client.publish(b'/test/mqttools/foo', b'bar', 0)
    await client.stop()

asyncio.run(publisher())

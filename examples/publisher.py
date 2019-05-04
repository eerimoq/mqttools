import asyncio
import mqttools

async def publisher():
    client = mqttools.Client('broker.hivemq.com', 1883, 'mqttools-publish')

    await client.start()
    await client.publish('/test/mqttools/foo', b'bar', 0)
    await client.stop()
    print("Successfully published b'bar' on /test/mqttools/foo.")

asyncio.run(publisher())

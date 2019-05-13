import asyncio
import mqttools


async def publisher():
    client = mqttools.Client('broker.hivemq.com', 1883)

    await client.start()
    client.publish('/test/mqttools/foo', b'bar')
    await client.stop()
    print("Successfully published b'bar' on /test/mqttools/foo.")


asyncio.run(publisher())

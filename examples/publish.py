import asyncio

import mqttools


async def publisher():
    async with mqttools.Client('localhost', 1883) as client:
        client.publish(mqttools.Message('/test/mqttools/foo', b'bar'))
        print("Successfully published b'bar' on /test/mqttools/foo.")


asyncio.run(publisher())

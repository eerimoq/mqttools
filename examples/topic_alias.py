import asyncio
import mqttools


async def publisher():
    client = mqttools.Client('broker.hivemq.com',
                             1883,
                             topic_aliases=[
                                 '/test/mqttools/foo'
                             ])

    await client.start()
    await client.publish('/test/mqttools/foo', b'sets-alias-in-broker', 0)
    await client.publish('/test/mqttools/foo', b'published-with-alias', 0)
    await client.publish('/test/mqttools/fie', b'not-using-alias', 0)
    await client.publish('/test/mqttools/fie', b'not-using-alias', 0)
    await client.stop()
    print("Successfully published b'bar' on /test/mqttools/foo.")


asyncio.run(publisher())

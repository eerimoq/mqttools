import asyncio
import mqttools


async def publisher():
    client = mqttools.Client('broker.hivemq.com',
                             1883,
                             topic_aliases=[
                                 '/test/mqttools/foo'
                             ])

    await client.start()
    client.publish('/test/mqttools/foo', b'sets-alias-in-broker')
    client.publish('/test/mqttools/foo', b'published-with-alias')
    client.publish('/test/mqttools/fie', b'not-using-alias')
    client.publish('/test/mqttools/fie', b'not-using-alias')
    await client.stop()
    print("Successfully published b'bar' on /test/mqttools/foo.")


asyncio.run(publisher())

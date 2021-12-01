import asyncio

import mqttools


async def publisher():
    client = mqttools.Client('localhost',
                             1883,
                             topic_aliases=[
                                 '/test/mqttools/foo'
                             ])

    await client.start()
    client.publish(mqttools.Message('/test/mqttools/foo', b'sets-alias-in-broker'))
    client.publish(mqttools.Message('/test/mqttools/foo', b'published-with-alias'))
    client.publish(mqttools.Message('/test/mqttools/fie', b'not-using-alias'))
    client.publish(mqttools.Message('/test/mqttools/fie', b'not-using-alias'))
    await client.stop()
    print("Successfully published b'bar' on /test/mqttools/foo.")


asyncio.run(publisher())

import asyncio

import mqttools


async def will():
    client = mqttools.Client('localhost',
                             1883,
                             will_topic='/my/will/topic',
                             will_message=b'my-will-message',
                             will_qos=0)

    await client.start()
    await client.stop()
    print("Successfully connected with will.")


asyncio.run(will())

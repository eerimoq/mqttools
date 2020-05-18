"""

 +--------+            +--------+            +-------------+
 |        |--- ping -->|        |--- ping -->|             |
 | client |            | broker |            | echo client |
 |        |<-- pong ---|        |<-- pong ---|             |
 +--------+            +--------+            +-------------+

"""

import time
import asyncio
import mqttools


BROKER_PORT = 10008


async def start_client():
    client = mqttools.Client('localhost', BROKER_PORT, connect_delays=[0.1])
    await client.start()

    return client


async def client_main():
    """Publish the current time to /ping and wait for the echo client to
    publish it back on /pong, with a one second interval.

    """

    client = await start_client()
    await client.subscribe('/pong')

    while True:
        print()
        message = str(int(time.time())).encode('ascii')
        print(f'client: Publishing {message} on /ping.')
        client.publish('/ping', message)
        topic, message = await client.messages.get()
        print(f'client: Got {message} on {topic}.')

        if topic is None:
            print('Client connection lost.')
            break

        await asyncio.sleep(1)


async def echo_client_main():
    """Wait for the client to publish to /ping, and publish /pong in
    response.

    """

    client = await start_client()
    await client.subscribe('/ping')

    while True:
        topic, message = await client.messages.get()
        print(f'echo_client: Got {message} on {topic}.')

        if topic is None:
            print('Echo client connection lost.')
            break

        print(f'echo_client: Publishing {message} on /pong.')
        client.publish('/pong', message)


async def broker_main():
    """The broker, serving both clients, forever.

    """

    broker = mqttools.Broker('localhost', BROKER_PORT)
    await broker.serve_forever()


async def main():
    await asyncio.gather(
        broker_main(),
        echo_client_main(),
        client_main()
    )


asyncio.run(main())

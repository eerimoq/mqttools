import asyncio
import mqttools


async def connect_and_subscribe(client):
    """Returns once connected to the broker and subscribed to a topic.

    """

    attempt = 1

    while True:
        try:
            await client.start()
            await client.subscribe('foobar')
            print('Connected and subscribed.')
            break
        except ConnectionRefusedError:
            print('TCP connect refused.')
        except mqttools.TimeoutError:
            print('MQTT connect acknowledge not received.')
        except mqttools.ConnectError as e:
            print(f'MQTT connect failed with reason {e}.')

        # Delay a while before the next connect attempt.
        delays = [1, 2, 4, 8]
        delay = delays[min(attempt, len(delays)) - 1]
        print(f'Waiting {delay} second(s) before next connection '
              f'attempt({attempt}).')
        await asyncio.sleep(delay)
        attempt += 1


async def handle_messages(client):
    while True:
        topic, message = await client.messages.get()
        print(f'Got {message} on {topic}.')

        if topic is None:
            print('Connection lost.')
            break


async def reconnector():
    client = mqttools.Client('localhost', 1883)

    while True:
        await connect_and_subscribe(client)
        await handle_messages(client)


asyncio.run(reconnector())

import asyncio

import mqttools


class Client(mqttools.Client):

    async def on_message(self, topic, message, retain, properties):
        await self._messages.put((topic, message, retain, properties))


async def subscriber():
    client = Client('localhost', 1883, subscriptions=['/test/mqttools/foo'])

    await client.start()

    print('Waiting for messages.')

    while True:
        topic, message, retain, properties = await client.messages.get()

        if topic is None:
            print('Broker connection lost!')
            break

        print(f'Topic:      {topic}')
        print(f'Message:    {message}')
        print(f'Retain:     {retain}')
        print(f'Properties:')

        for name, value in properties.items():
            print(f'  {name}: {value}')


asyncio.run(subscriber())

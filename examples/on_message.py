import asyncio

import mqttools


class Client(mqttools.Client):

    async def on_message(self, message):
        await self._messages.put((message.topic,
                                  message.message,
                                  message.retain,
                                  message.response_topic))


async def subscriber():
    client = Client('localhost', 1883, subscriptions=['/test/mqttools/foo'])

    await client.start()

    print('Waiting for messages.')

    while True:
        topic, message, retain, response_topic = await client.messages.get()

        if topic is None:
            print('Broker connection lost!')
            break

        print(f'Topic:         {topic}')
        print(f'Message:       {message}')
        print(f'Retain:        {retain}')
        print(f'ResponseTopic: {response_topic}')


asyncio.run(subscriber())

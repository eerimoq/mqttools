import asyncio

import mqttools

HOST = 'localhost'
PORT = 1883


async def main():
    client = mqttools.Client(HOST, PORT)
    await client.start()
    print(f'Connected to {HOST}:{PORT}.')
    await client.subscribe('/mqttools/incrementer/value/request')

    print('Subscribed to topic /mqttools/incrementer/value/request.')

    while True:
        message = await client.messages.get()

        if message is None:
            print('Broker connection lost!')
            break

        count = int(message.message)
        print(f'Request count:  {count}')
        count += 1
        print(f'Response count: {count}')
        client.publish(mqttools.Message('/mqttools/counter-client/value/response',
                                        str(count).encode('ascii')))


asyncio.run(main())

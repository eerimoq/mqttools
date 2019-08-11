import asyncio
import mqttools
from uuid import uuid1


async def resume_session():
    client = mqttools.Client('localhost',
                             1883,
                             client_id='mqttools-{}'.format(uuid1().node),
                             session_expiry_interval=15)

    try:
        await client.start(resume_session=True)
        print('Session resumed.')
    except mqttools.SessionResumeError:
        print('No session to resume. Subscribing to topics.')

        # Subscribe to three topics in parallel.
        print('Subscribing to topics.')
        await asyncio.gather(
            client.subscribe('$SYS/#'),
            client.subscribe('/test/mqttools/foo')
        )

    print('Waiting for messages.')

    while True:
        topic, message = await client.messages.get()

        if topic is None:
            print('Broker connection lost!')
            break

        print(f'Topic:   {topic}')
        print(f'Message: {message}')

import logging
logging.basicConfig(level=logging.DEBUG)

asyncio.run(resume_session())

/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019, Erik Moqvist
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * This file is part of the mqttools project.
 */

#include "mqttools.h"

int main()
{
    int res;
    ssize_t i;
    ssize_t size;
    struct mqttools_client_t client;
    char topic[128];
    uint8_t message[256];

    mqttools_client_init(&client, "broker.hivemq.com", 1883);

    res = mqttools_client_start(&client, false);

    if (res != MQTTOOLS_OK) {
        return (2);
    }

    res = mqttools_client_subscribe(&client, "/test/mqttools/#");

    if (res != MQTTOOLS_OK) {
        return (3);
    }

    printf("Successfully subscribed to '/test/mqttools/#'.\n");

    while (true) {
        size = mqttools_client_read_message(&client,
                                            &topic[0],
                                            sizeof(topic),
                                            &message[0],
                                            sizeof(message));

        if (size < 0) {
            return (4);
        }

        printf("Topic:   %s\n", &topic[0]);
        printf("Message: ");

        for (i = 0; i < size; i++) {
            printf("%02x", message[i]);
        }

        printf("\n");
    }

    res = mqttools_client_stop(&client);

    if (res != MQTTOOLS_OK) {
        return (5);
    }

    return (0);
}

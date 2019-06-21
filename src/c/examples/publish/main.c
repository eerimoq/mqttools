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
    struct mqttools_client_t client;

    mqttools_client_init(&client, "broker.hivemq.com", 1883, NULL);

    res = mqttools_client_start(&client, false);

    if (res != MQTTOOLS_OK) {
        return (2);
    }

    res = mqttools_client_publish(&client,
                                  "/test/mqttools/foo",
                                  (const uint8_t *)"bar",
                                  3);

    if (res != MQTTOOLS_OK) {
        return (3);
    }

    printf("Successfully published b'bar' on '/test/mqttools/foo'.");

    res = mqttools_client_stop(&client);

    if (res != MQTTOOLS_OK) {
        return (4);
    }

    return (0);
}

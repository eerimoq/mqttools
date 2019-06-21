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

#ifndef MQTTOOLS_H
#define MQTTOOLS_H

/* Error codes. */
#define MQTTOOLS_OK                             0
#define MQTTOOLS_SESSION_RESUME                 1

struct mqttools_client_t {
    const char *host_p;
    int port;
    const char *client_id_p;
    const char *will_topic_p;
    const uint8_t *will_message_p;
    int will_qos;
    int keep_alive_s;
    int response_timeout;
    int topic_aliases;
    int topic_alias_maximum;
    int session_expiry_interval;
    const char **subscriptions_pp;
    int connect_delays;
};

void mqttools_client_init(struct mqttools_client_t *self_p,
                          const char *host_p,
                          int port,
                          const char *client_id_p,
                          const char *will_topic_p,
                          const uint8_t *will_message_p,
                          int will_qos,
                          int keep_alive_s,
                          int response_timeout,
                          const char **topic_aliases_pp,
                          int topic_alias_maximum,
                          int session_expiry_interval,
                          const char **subscriptions_pp,
                          int *connect_delays_p);

int mqttools_client_start(struct mqttools_client_t *self_p,
                          bool resume_session);

void mqttools_client_stop(struct mqttools_client_t *self_p);

int mqttools_client_subscribe(struct mqttools_client_t *self_p,
                              const char *topic_p);

int mqttools_client_unsubscribe(struct mqttools_client_t *self_p,
                                const char *topic_p);

int mqttools_client_publish(struct mqttools_client_t *self_p,
                            const char *topic_p,
                            const uint8_t *message_p);

#endif

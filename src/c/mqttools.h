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
#define MQTTOOLS_DISCONNECTED                   2

struct mqttools_client_t {
    const char *host_p;
    int port;
    const char *client_id_p;
    struct {
        const char *topic_p;
        const uint8_t *message_p;
        size_t message_size;
        int qos;
    } will;
    int keep_alive_s;
    int response_timeout;
    int topic_aliases;
    int topic_alias_maximum;
    int session_expiry_interval;
    const char **subscriptions_pp;
    int *connect_delays_p;
};

void mqttools_client_init(struct mqttools_client_t *self_p,
                          const char *host_p,
                          int port);

void mqttools_client_set_client_id(struct mqttools_client_t *self_p,
                                   const char *client_id_p);

void mqttools_client_set_will(struct mqttools_client_t *self_p,
                              const char *topic_p,
                              const uint8_t *message_p,
                              int qos);

void mqttools_client_set_response_timeout(struct mqttools_client_t *self_p,
                                          int response_timeout);

void mqttools_client_set_topic_aliases(struct mqttools_client_t *self_p,
                                       const char **topic_aliases_pp);

void mqttools_client_set_topic_alias_maximum(struct mqttools_client_t *self_p,
                                             int topic_alias_maximum);

void mqttools_client_set_session_expiry_interval(struct mqttools_client_t *self_p,
                                                 int session_expiry_interval);

void mqttools_client_set_subscriptions(struct mqttools_client_t *self_p,
                                       const char **subscriptions_pp);

void mqttools_client_set_connect_delays(struct mqttools_client_t *self_p,
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

ssize_t mqttools_client_read_message(struct mqttools_client_t *self_p,
                                     char *topic_p,
                                     size_t topic_size,
                                     uint8_t *message_p,
                                     size_t message_size);

#endif

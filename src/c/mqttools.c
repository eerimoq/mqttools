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


/* Connection flags. */
#define CLEAN_START     0x02
#define WILL_FLAG       0x04
#define WILL_QOS_1      0x08
#define WILL_QOS_2      0x10
#define WILL_RETAIN     0x20
#define PASSWORD_FLAG   0x40
#define USER_NAME_FLAG  0x80


/* Control packet types. */
enum control_packet_type_t {
    control_packet_type_connect_t = 1,
    control_packet_type_connack_t = 2,
    control_packet_type_publish_t = 3,
    control_packet_type_puback_t = 4,
    control_packet_type_pubrec_t = 5,
    control_packet_type_pubrel_t = 6,
    control_packet_type_pubcomp_t = 7,
    control_packet_type_subscribe_t = 8,
    control_packet_type_suback_t = 9,
    control_packet_type_unsubscribe_t = 10,
    control_packet_type_unsuback_t = 11,
    control_packet_type_pingreq_t = 12,
    control_packet_type_pingresp_t = 13,
    control_packet_type_disconnect_t = 14,
    control_packet_type_auth_t = 15
};

enum connect_reason_code_t {
    connect_reason_code_SUCCESS_t = 0,
    connect_reason_code_v3_1_1_unacceptable_protocol_version_t = 1,
    connect_reason_code_v3_1_1_identifier_rejected_t = 2,
    connect_reason_code_v3_1_1_server_unavailable_t = 3,
    connect_reason_code_v3_1_1_bad_user_name_or_password_t = 4,
    connect_reason_code_v3_1_1_not_authorized_t = 5,
    connect_reason_code_unspecified_error_t = 128,
    connect_reason_code_malformed_packet_t = 129,
    connect_reason_code_protocol_error_t = 130,
    connect_reason_code_implementation_specific_error_t = 131,
    connect_reason_code_unsupported_protocol_version_t = 132,
    connect_reason_code_client_identifier_not_valid_t = 133,
    connect_reason_code_bad_user_name_or_password_t = 134,
    connect_reason_code_not_authorized_t = 135,
    connect_reason_code_server_unavailable_t = 136,
    connect_reason_code_server_busy_t = 137,
    connect_reason_code_banned_t = 138,
    connect_reason_code_bad_authentication_method_t = 140,
    connect_reason_code_topic_name_invalid_t = 144,
    connect_reason_code_packet_too_large_t = 149,
    connect_reason_code_quota_exceeded_t = 151,
    connect_reason_code_payload_format_invalid_t = 153,
    connect_reason_code_retain_not_supported_t = 154,
    connect_reason_code_qos_not_supported_t = 155,
    connect_reason_code_use_another_server_t = 156,
    connect_reason_code_server_moved_t = 157,
    connect_reason_code_connection_rate_exceeded_t = 159
};

enum disconnect_reason_code_t {
    disconnect_reason_code_normal_disconnection_t = 0,
    disconnect_reason_code_disconnect_with_will_message_t = 4,
    disconnect_reason_code_unspecified_error_t = 128,
    disconnect_reason_code_malformed_packet_t = 129,
    disconnect_reason_code_protocol_error_t = 130,
    disconnect_reason_code_implementation_specific_error_t = 131,
    disconnect_reason_code_not_authorized_t = 135,
    disconnect_reason_code_server_busy_t = 137,
    disconnect_reason_code_server_shutting_down_t = 139,
    disconnect_reason_code_keep_alive_timeout_t = 141,
    disconnect_reason_code_session_taken_over_t = 142,
    disconnect_reason_code_topic_filter_invalid_t = 143,
    disconnect_reason_code_topic_name_invalid_t = 144,
    disconnect_reason_code_receive_maximum_exceeded_t = 147,
    disconnect_reason_code_topic_alias_invalid_t = 148,
    disconnect_reason_code_packet_too_large_t = 149,
    disconnect_reason_code_message_rate_too_high_t = 150,
    disconnect_reason_code_quota_exceeded_t = 151,
    disconnect_reason_code_administrative_action_t = 152,
    disconnect_reason_code_payload_format_invalid_t = 153,
    disconnect_reason_code_retain_not_supported_t = 154,
    disconnect_reason_code_qos_not_supported_t = 155,
    disconnect_reason_code_use_another_server_t = 156,
    disconnect_reason_code_server_moved_t = 157,
    disconnect_reason_code_shared_subscriptions_not_supported_t = 158,
    disconnect_reason_code_connection_rate_exceeded_t = 159,
    disconnect_reason_code_maximum_connect_time_t = 160,
    disconnect_reason_code_subscription_identifiers_not_supported_t = 161,
    disconnect_reason_code_wildcard_subscriptions_not_supported_t = 162
};

enum suback_reason_code_t {
    subsck_reason_code_granted_qos_0_t = 0,
    subsck_reason_code_granted_qos_1_t = 1,
    subsck_reason_code_granted_qos_2_t = 2,
    subsck_reason_code_unspecified_error_t = 128,
    subsck_reason_code_implementation_specific_error_t = 131,
    subsck_reason_code_not_authorized_t = 135,
    subsck_reason_code_topic_filter_invalid_t = 143,
    subsck_reason_code_packet_identifier_in_use_t = 145,
    subsck_reason_code_quota_exceeded_t = 151,
    subsck_reason_code_shared_subscriptions_not_supported_t = 158,
    subsck_reason_code_subscription_identifiers_not_supported_t = 161,
    subsck_reason_code_wildcard_subscriptions_not_supported_t = 162
};

enum unsuback_reason_code_t {
    unsubsck_reason_code_success_t = 0,
    unsubsck_reason_code_no_subscription_existed_t = 17,
    unsubsck_reason_code_unspecified_error_t = 128,
    unsubsck_reason_code_implementation_specific_error_t = 131,
    unsubsck_reason_code_not_authorized_t = 135,
    unsubsck_reason_code_topic_filter_invalid_t = 143,
    unsubsck_reason_code_packet_identifier_in_use_t = 145
};

enum property_ids_t {
    property_ids_payload_format_indicator_t = 1,
    property_ids_message_expiry_interval_t = 2,
    property_ids_content_type_t = 3,
    property_ids_response_topic_t = 8,
    property_ids_correlation_data_t = 9,
    property_ids_subscription_identifier_t = 11,
    property_ids_session_expiry_interval_t = 17,
    property_ids_assigned_client_identifier_t = 18,
    property_ids_server_keep_alive_t = 19,
    property_ids_authentication_method_t = 21,
    property_ids_authentication_data_t = 22,
    property_ids_request_problem_information_t = 23,
    property_ids_will_delay_interval_t = 24,
    property_ids_request_response_information_t = 25,
    property_ids_response_information_t = 26,
    property_ids_server_reference_t = 28,
    property_ids_reason_string_t = 31,
    property_ids_receive_maximum_t = 33,
    property_ids_topic_alias_maximum_t = 34,
    property_ids_topic_alias_t = 35,
    property_ids_maximum_qos_t = 36,
    property_ids_retain_available_t = 37,
    property_ids_user_property_t = 38,
    property_ids_maximum_packet_size_t = 39,
    property_ids_wildcard_subscription_available_t = 40,
    property_ids_subscription_identifier_available_t = 41,
    property_ids_shared_subscription_available_t = 42
};

/* MQTT 5.0 */
#define PROTOCOL_VERSION 5

#define MAXIMUM_PACKET_SIZE (268435455)  /* (128 ^ 4 - 1) */


static int pack_u32(uint8_t *dst_p, uint32_t value, size_t size)
{
    if (size < 4) {
        return (-1);
    }

    dst_p[0] = (value >> 24);
    dst_p[1] = (value >> 16);
    dst_p[2] = (value >> 8);
    dst_p[3] = (value >> 0);

    return (0);
}


static int unpack_u32(uint32_t *dst_p, uint8_t *src_p, size_t size)
{
    if (size < 4) {
        return (-1);
    }

    *dst_p = (src_p[0]);

    return (0);
}

static ssize_t pack_connect(uint8_t *dst_p,
                            const char *client_id_p,
                            bool clean_start,
                            const char *will_topic_p,
                            const uint8_t *will_message_p,
                            int will_qos,
                            int keep_alive_s,
                            int properties)
{
    uint8_t flags;
    int payload_length;

    flags = 0;

    if (clean_start) {
        flags |= CLEAN_START;
    }

    payload_length = strlen(client_id_p) + 2;

    if (will_topic_p != NULL) {
        flags |= WILL_FLAG;

        if (will_qos == 1) {
            flags |= WILL_QOS_1;
        } else if (will_qos == 2) {
            flags |= WILL_QOS_2;
        }

        payload_length++;
        packed_will_topic = pack_string(will_topic_p);
        payload_length += len(packed_will_topic);
        payload_length += (will_message_size + 2);
    }

    properties = pack_properties('CONNECT', properties);
    packed = pack_fixed_header(control_packet_type_connect_t,
                               0,
                               10 + payload_length + len(properties));
    packed += pack_string('MQTT');
    packed += struct.pack('>BBH',
                          PROTOCOL_VERSION,
                          flags,
                          keep_alive_s);
    packed += properties;
    packed += pack_string(client_id);

    if (flags & WILL_FLAG) {
        packed += pack_variable_integer(0);
        packed += packed_will_topic;
        packed += pack_binary(will_message);
    }

    return (packed);
}

static void *main(void *arg_p)
{
    timerfd keep_alive;
    eventfd event;

    epoll();
}

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
                          int *connect_delays_p)
{
}

int mqttools_client_start(struct mqttools_client_t *self_p,
                          bool resume_session)
{
    pthread_create(main);
    socket();
    connect(resume_session);
    subscribe();
}

void mqttools_client_stop(struct mqttools_client_t *self_p)
{
    disconnect();
    close();
    join();
}

int mqttools_client_subscribe(struct mqttools_client_t *self_p,
                              const char *topic_p)
{
    lock();
    write();
    read();
    unlock();
}

int mqttools_client_unsubscribe(struct mqttools_client_t *self_p,
                                const char *topic_p)
{
    lock();
    write();
    read();
    unlock();
}

int mqttools_client_publish(struct mqttools_client_t *self_p,
                            const char *topic_p,
                            const uint8_t *message_p)
{
    lock();
    write();
    read();
    unlock();
}

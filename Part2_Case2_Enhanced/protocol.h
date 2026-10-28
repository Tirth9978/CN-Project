#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include "common_net.h"

#define PROTOCOL_MAGIC 0x434E534F /* 'CNSO' */
#define MAX_FILES_PER_TOPIC 16384

/* Message Opcodes */
#define MSG_INIT_REQUEST      0x01
#define MSG_SESSION_MANIFEST  0x02
#define MSG_FILE_START        0x03
#define MSG_DATA_PAYLOAD      0x04
#define MSG_FILE_COMPLETE     0x05
#define MSG_RESUME_REQUEST    0x06
#define MSG_RESUME_ACK        0x07
#define MSG_TOPIC_COMPLETE    0x08
#define MSG_TASK_REQUEST      0x10
#define MSG_TASK_ASSIGN       0x11
#define MSG_ERROR             0xFF

/* Flags */
#define FLAG_FINAL_FILE       0x01
#define FLAG_SINGLE_TASK      0x02

/* Status Codes */
#define STATUS_OK             0
#define STATUS_UNKNOWN_TOPIC  404
#define STATUS_SESSION_EXPIRED 410
#define STATUS_INVALID_OFFSET 416
#define STATUS_NO_MORE_TASKS  204
#define STATUS_ERROR          500

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;         /* Magic: 0x434E534F */
    uint8_t  msg_type;      /* Opcode */
    uint8_t  flags;         /* Control flags */
    uint16_t status_code;   /* Status */
    uint32_t session_id;    /* Session ID */
    uint32_t file_index;    /* Current File Index */
    uint64_t byte_offset;   /* Byte Offset within File */
    uint64_t payload_len;   /* Binary payload length */
} ProtocolHeader;
#pragma pack(pop)

static inline void header_to_net(ProtocolHeader *h) {
    h->magic        = htonl(h->magic);
    h->status_code  = htons(h->status_code);
    h->session_id   = htonl(h->session_id);
    h->file_index   = htonl(h->file_index);
    h->byte_offset  = htobe64(h->byte_offset);
    h->payload_len  = htobe64(h->payload_len);
}

static inline void header_from_net(ProtocolHeader *h) {
    h->magic        = ntohl(h->magic);
    h->status_code  = ntohs(h->status_code);
    h->session_id   = ntohl(h->session_id);
    h->file_index   = ntohl(h->file_index);
    h->byte_offset  = be64toh(h->byte_offset);
    h->payload_len  = be64toh(h->payload_len);
}

#endif /* PROTOCOL_H */

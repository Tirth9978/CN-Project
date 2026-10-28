#include "common_net.h"
#include "topic_manager.h"
#include "fault_injector.h"
#include "session_manager.h"
#include "protocol.h"
#include <fcntl.h>

static volatile sig_atomic_t g_running = 1;
static TopicRegistry g_topic_reg;
static SessionRegistry g_session_reg;

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

typedef struct {
    int client_socket;
    struct sockaddr_in client_addr;
    TopicRegistry *topic_reg;
    SessionRegistry *session_reg;
    double failure_prob;
} WorkerContext;

static void handle_client_case2(int client_fd, TopicRegistry *treg, SessionRegistry *sreg, double failure_prob) {
    ProtocolHeader req_hdr;
    if (recv_exact(client_fd, &req_hdr, sizeof(req_hdr)) <= 0) {
        return;
    }
    header_from_net(&req_hdr);

    if (req_hdr.magic != PROTOCOL_MAGIC) {
        fprintf(stderr, "[SERVER] Invalid magic header: 0x%08X\n", req_hdr.magic);
        return;
    }

    ActiveSession *session = NULL;
    const TopicRecord *topic = NULL;
    uint32_t start_file_idx = 0;
    uint64_t start_byte_offset = 0;

    if (req_hdr.msg_type == MSG_INIT_REQUEST) {
        if (req_hdr.payload_len >= MAX_TOPIC_NAME_LEN || req_hdr.payload_len == 0) {
            return;
        }

        char topic_name[MAX_TOPIC_NAME_LEN];
        if (recv_exact(client_fd, topic_name, req_hdr.payload_len) <= 0) {
            return;
        }
        topic_name[req_hdr.payload_len] = '\0';

        if (strstr(topic_name, "..") || strchr(topic_name, '/') || strchr(topic_name, '\\')) {
            ProtocolHeader err_hdr = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_ERROR,
                .status_code = STATUS_ERROR,
                .payload_len = 0
            };
            header_to_net(&err_hdr);
            send_exact(client_fd, &err_hdr, sizeof(err_hdr));
            return;
        }

        topic = topic_registry_find(treg, topic_name);
        if (!topic) {
            ProtocolHeader err_hdr = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_ERROR,
                .status_code = STATUS_UNKNOWN_TOPIC,
                .payload_len = 0
            };
            header_to_net(&err_hdr);
            send_exact(client_fd, &err_hdr, sizeof(err_hdr));
            return;
        }

        session = session_create(sreg, topic->topic_name, topic->file_count);
        if (!session) {
            return;
        }

        ProtocolHeader manifest = {
            .magic = PROTOCOL_MAGIC,
            .msg_type = MSG_SESSION_MANIFEST,
            .flags = 0,
            .status_code = STATUS_OK,
            .session_id = session->session_id,
            .file_index = topic->file_count,
            .byte_offset = 0,
            .payload_len = topic->total_topic_bytes
        };
        header_to_net(&manifest);
        if (send_exact(client_fd, &manifest, sizeof(manifest)) < 0) {
            return;
        }

        start_file_idx = 0;
        start_byte_offset = 0;

    } else if (req_hdr.msg_type == MSG_RESUME_REQUEST) {
        session = session_find(sreg, req_hdr.session_id);
        if (!session) {
            ProtocolHeader err_hdr = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_ERROR,
                .status_code = STATUS_SESSION_EXPIRED,
                .session_id = req_hdr.session_id,
                .payload_len = 0
            };
            header_to_net(&err_hdr);
            send_exact(client_fd, &err_hdr, sizeof(err_hdr));
            return;
        }

        topic = topic_registry_find(treg, session->topic);
        if (!topic) {
            return;
        }

        if (req_hdr.file_index >= topic->file_count) {
            ProtocolHeader err_hdr = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_ERROR,
                .status_code = STATUS_INVALID_OFFSET,
                .session_id = session->session_id,
                .payload_len = 0
            };
            header_to_net(&err_hdr);
            send_exact(client_fd, &err_hdr, sizeof(err_hdr));
            return;
        }

        start_file_idx = req_hdr.file_index;
        start_byte_offset = req_hdr.byte_offset;

        ProtocolHeader ack = {
            .magic = PROTOCOL_MAGIC,
            .msg_type = MSG_RESUME_ACK,
            .flags = 0,
            .status_code = STATUS_OK,
            .session_id = session->session_id,
            .file_index = start_file_idx,
            .byte_offset = start_byte_offset,
            .payload_len = 0
        };
        header_to_net(&ack);
        if (send_exact(client_fd, &ack, sizeof(ack)) < 0) {
            return;
        }
    } else {
        return;
    }

    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(uintptr_t)pthread_self();
    char chunk[BUFFER_SIZE];

    int session_will_fail = 0;
    uint32_t fail_file_idx = 0;
    uint64_t fault_offset = 0;

    if (failure_prob > 0.0 && topic->file_count > start_file_idx) {
        double r = (double)rand_r(&seed) / (double)RAND_MAX;
        if (r <= failure_prob) {
            session_will_fail = 1;
            uint32_t rem = topic->file_count - start_file_idx;
            fail_file_idx = start_file_idx + (uint32_t)(rand_r(&seed) % rem);
            const FileRecord *ff = &topic->files[fail_file_idx];
            uint64_t cur_off = (fail_file_idx == start_file_idx) ? start_byte_offset : 0;
            if (ff->file_size > cur_off) {
                double frac = (double)rand_r(&seed) / (double)RAND_MAX;
                fault_offset = cur_off + (uint64_t)(frac * (double)(ff->file_size - cur_off));
                if (ff->file_size - cur_off < 1024) {
                    fault_offset = cur_off + (ff->file_size - cur_off) / 2;
                }
            } else {
                fault_offset = cur_off;
            }
        }
    }

    for (uint32_t i = start_file_idx; i < topic->file_count; i++) {
        const FileRecord *f = &topic->files[i];
        uint64_t current_offset = (i == start_file_idx) ? start_byte_offset : 0;

        if (current_offset >= f->file_size && f->file_size > 0) {
            session_update_progress(session, i, f->file_size, 1);
            continue;
        }

        ProtocolHeader start_hdr = {
            .magic = PROTOCOL_MAGIC,
            .msg_type = MSG_FILE_START,
            .flags = (i == topic->file_count - 1) ? 0x01 : 0x00,
            .status_code = STATUS_OK,
            .session_id = session->session_id,
            .file_index = i,
            .byte_offset = current_offset,
            .payload_len = strlen(f->filename)
        };
        ProtocolHeader net_start = start_hdr;
        header_to_net(&net_start);

        if (send_exact(client_fd, &net_start, sizeof(net_start)) < 0) return;
        if (send_exact(client_fd, f->filename, start_hdr.payload_len) < 0) return;

        int file_fd = open(f->file_path, O_RDONLY);
        if (file_fd < 0) {
            perror("open source file");
            return;
        }

        if (current_offset > 0) {
            if (lseek(file_fd, (off_t)current_offset, SEEK_SET) == (off_t)-1) {
                perror("lseek");
                close(file_fd);
                return;
            }
        }

        int will_fail = (session_will_fail && (i == fail_file_idx));

        while (current_offset < f->file_size) {
            size_t to_read = sizeof(chunk);
            if (f->file_size - current_offset < to_read) {
                to_read = (size_t)(f->file_size - current_offset);
            }

            if (will_fail && (current_offset + to_read >= fault_offset)) {
                size_t partial = (fault_offset > current_offset) ? (size_t)(fault_offset - current_offset) : 0;
                if (partial > 0) {
                    ssize_t bread = read(file_fd, chunk, partial);
                    if (bread > 0) {
                        ProtocolHeader data_hdr = {
                            .magic = PROTOCOL_MAGIC,
                            .msg_type = MSG_DATA_PAYLOAD,
                            .status_code = STATUS_OK,
                            .session_id = session->session_id,
                            .file_index = i,
                            .byte_offset = current_offset,
                            .payload_len = (uint64_t)bread
                        };
                        header_to_net(&data_hdr);
                        send_exact(client_fd, &data_hdr, sizeof(data_hdr));
                        send_exact(client_fd, chunk, (size_t)bread);
                        current_offset += (uint64_t)bread;
                    }
                }
                close(file_fd);
                session_update_progress(session, i, fault_offset, 0);
                execute_abrupt_disconnect(client_fd, topic->topic_name, i, fault_offset);
                return;
            }

            ssize_t bread = read(file_fd, chunk, to_read);
            if (bread <= 0) break;

            ProtocolHeader data_hdr = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_DATA_PAYLOAD,
                .status_code = STATUS_OK,
                .session_id = session->session_id,
                .file_index = i,
                .byte_offset = current_offset,
                .payload_len = (uint64_t)bread
            };
            header_to_net(&data_hdr);
            if (send_exact(client_fd, &data_hdr, sizeof(data_hdr)) < 0) {
                close(file_fd);
                return;
            }
            if (send_exact(client_fd, chunk, (size_t)bread) < 0) {
                close(file_fd);
                return;
            }

            current_offset += (uint64_t)bread;
            session_update_progress(session, i, current_offset, (current_offset >= f->file_size) ? 1 : 0);
        }
        close(file_fd);

        ProtocolHeader comp_hdr = {
            .magic = PROTOCOL_MAGIC,
            .msg_type = MSG_FILE_COMPLETE,
            .status_code = STATUS_OK,
            .session_id = session->session_id,
            .file_index = i,
            .byte_offset = f->file_size,
            .payload_len = 0
        };
        header_to_net(&comp_hdr);
        if (send_exact(client_fd, &comp_hdr, sizeof(comp_hdr)) < 0) return;
    }

    ProtocolHeader topic_comp = {
        .magic = PROTOCOL_MAGIC,
        .msg_type = MSG_TOPIC_COMPLETE,
        .status_code = STATUS_OK,
        .session_id = session->session_id,
        .payload_len = 0
    };
    header_to_net(&topic_comp);
    send_exact(client_fd, &topic_comp, sizeof(topic_comp));
}

static void *client_thread_entry(void *arg) {
    WorkerContext *ctx = (WorkerContext *)arg;
    pthread_detach(pthread_self());

    handle_client_case2(ctx->client_socket, ctx->topic_reg, ctx->session_reg, ctx->failure_prob);

    close(ctx->client_socket);
    free(ctx);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <port> <topics_root_dir> <failure_probability>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    const char *topics_root_dir = argv[2];
    double failure_prob = atof(argv[3]);

    if (failure_prob < 0.0 || failure_prob > 1.0) {
        fprintf(stderr, "Error: failure_probability must be between 0.0 and 1.0\n");
        return 1;
    }

    setup_signal_handlers();

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (topic_registry_init(&g_topic_reg, topics_root_dir) != 0) {
        fprintf(stderr, "Failed to initialize topic registry from %s\n", topics_root_dir);
        return 1;
    }

    session_registry_init(&g_session_reg);
    topic_registry_print_summary(&g_topic_reg);

    int listen_fd = create_server_socket(port);
    if (listen_fd < 0) {
        fprintf(stderr, "Failed to bind and listen on port %d\n", port);
        topic_registry_destroy(&g_topic_reg);
        session_destroy_all(&g_session_reg);
        return 1;
    }

    printf("[SERVER] Part 2 Case 2 Server listening on port %d (Failure Prob: %.2f)\n", port, failure_prob);

    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (!g_running) break;
            perror("accept");
            break;
        }

        WorkerContext *ctx = malloc(sizeof(WorkerContext));
        if (!ctx) {
            perror("malloc worker context");
            close(client_fd);
            continue;
        }
        ctx->client_socket = client_fd;
        ctx->client_addr = client_addr;
        ctx->topic_reg = &g_topic_reg;
        ctx->session_reg = &g_session_reg;
        ctx->failure_prob = failure_prob;

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread_entry, ctx) != 0) {
            perror("pthread_create");
            close(client_fd);
            free(ctx);
        }
    }

    printf("\n[SERVER] Shutting down gracefully...\n");
    close(listen_fd);
    topic_registry_destroy(&g_topic_reg);
    session_destroy_all(&g_session_reg);
    return 0;
}

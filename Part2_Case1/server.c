#include "common_net.h"
#include "topic_manager.h"
#include "fault_injector.h"
#include <fcntl.h>

static volatile sig_atomic_t g_running = 1;
static TopicRegistry g_registry;

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

typedef struct {
    int client_socket;
    struct sockaddr_in client_addr;
    TopicRegistry *registry;
    double failure_prob;
} WorkerContext;

static void handle_client_case1(int client_fd, TopicRegistry *reg, double failure_prob) {
    char req_line[MAX_LINE_LEN];
    ssize_t line_len = recv_line(client_fd, req_line, sizeof(req_line));
    if (line_len <= 0) {
        return;
    }

    if (strncmp(req_line, "GET ", 4) != 0) {
        const char *err = "ERROR INVALID_COMMAND\n";
        send_exact(client_fd, err, strlen(err));
        return;
    }

    char *topic_query = req_line + 4;
    while (*topic_query == ' ') topic_query++;

    if (strstr(topic_query, "..") != NULL || strchr(topic_query, '/') != NULL || strchr(topic_query, '\\') != NULL) {
        const char *err = "ERROR SECURITY_VIOLATION\n";
        send_exact(client_fd, err, strlen(err));
        return;
    }

    const TopicRecord *topic = topic_registry_find(reg, topic_query);
    if (!topic) {
        const char *err = "ERROR UNKNOWN_TOPIC\n";
        send_exact(client_fd, err, strlen(err));
        return;
    }

    char resp_header[MAX_LINE_LEN];
    snprintf(resp_header, sizeof(resp_header), "OK %u %lu\n",
             topic->file_count, (unsigned long)topic->total_topic_bytes);
    if (send_exact(client_fd, resp_header, strlen(resp_header)) < 0) {
        return;
    }

    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(uintptr_t)pthread_self();
    char chunk[BUFFER_SIZE];

    /* Session-level fault injection: evaluates whether this client connection session will be interrupted */
    int session_will_fail = 0;
    uint32_t fail_file_idx = 0;
    uint64_t fault_offset = 0;

    if (failure_prob > 0.0 && topic->file_count > 0) {
        double r = (double)rand_r(&seed) / (double)RAND_MAX;
        if (r <= failure_prob) {
            session_will_fail = 1;
            fail_file_idx = (uint32_t)(rand_r(&seed) % topic->file_count);
            const FileRecord *ff = &topic->files[fail_file_idx];
            if (ff->file_size > 0) {
                double frac = (double)rand_r(&seed) / (double)RAND_MAX;
                fault_offset = (uint64_t)(frac * (double)ff->file_size);
            }
            if (ff->file_size < 1024) {
                fault_offset = ff->file_size / 2;
            }
        }
    }

    for (uint32_t i = 0; i < topic->file_count; i++) {
        const FileRecord *f = &topic->files[i];
        int will_fail = (session_will_fail && (i == fail_file_idx));

        char file_header[MAX_LINE_LEN];
        snprintf(file_header, sizeof(file_header), "FILE %s %lu\n",
                 f->filename, (unsigned long)f->file_size);
        if (send_exact(client_fd, file_header, strlen(file_header)) < 0) {
            return;
        }

        int file_fd = open(f->file_path, O_RDONLY);
        if (file_fd < 0) {
            perror("open source file");
            return;
        }

        uint64_t file_bytes_sent = 0;
        while (file_bytes_sent < f->file_size) {
            size_t to_read = sizeof(chunk);
            if (f->file_size - file_bytes_sent < to_read) {
                to_read = (size_t)(f->file_size - file_bytes_sent);
            }

            if (will_fail && (file_bytes_sent + to_read >= fault_offset)) {
                /* Transmit partial chunk up to fault offset then abruptly sever connection */
                size_t partial = (fault_offset > file_bytes_sent) ? (size_t)(fault_offset - file_bytes_sent) : 0;
                if (partial > 0) {
                    ssize_t bread = read(file_fd, chunk, partial);
                    if (bread > 0) {
                        send_exact(client_fd, chunk, (size_t)bread);
                    }
                }
                close(file_fd);
                execute_abrupt_disconnect(client_fd, topic->topic_name, i, fault_offset);
                return; /* Disconnected abruptly via SO_LINGER RST */
            }

            ssize_t bytes_read = read(file_fd, chunk, to_read);
            if (bytes_read <= 0) {
                break;
            }

            if (send_exact(client_fd, chunk, (size_t)bytes_read) < 0) {
                close(file_fd);
                return;
            }
            file_bytes_sent += (uint64_t)bytes_read;
        }
        close(file_fd);
    }

    const char *done_msg = "DONE\n";
    send_exact(client_fd, done_msg, strlen(done_msg));
}

static void *client_thread_entry(void *arg) {
    WorkerContext *ctx = (WorkerContext *)arg;
    pthread_detach(pthread_self());

    handle_client_case1(ctx->client_socket, ctx->registry, ctx->failure_prob);

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

    if (topic_registry_init(&g_registry, topics_root_dir) != 0) {
        fprintf(stderr, "Failed to initialize topic registry from %s\n", topics_root_dir);
        return 1;
    }

    topic_registry_print_summary(&g_registry);

    int listen_fd = create_server_socket(port);
    if (listen_fd < 0) {
        fprintf(stderr, "Failed to bind and listen on port %d\n", port);
        topic_registry_destroy(&g_registry);
        return 1;
    }

    printf("[SERVER] Part 2 Case 1 Server listening on port %d (Failure Prob: %.2f)\n", port, failure_prob);
    fflush(stdout);

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
        ctx->registry = &g_registry;
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
    topic_registry_destroy(&g_registry);
    return 0;
}

#include "common_net.h"
#include "topic_manager.h"
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
} WorkerContext;

static void handle_client_part1(int client_fd, TopicRegistry *reg) {
    char req_line[MAX_LINE_LEN];
    ssize_t line_len = recv_line(client_fd, req_line, sizeof(req_line));
    if (line_len <= 0) {
        return;
    }

    /* Validate command syntax: "GET <topic>" */
    if (strncmp(req_line, "GET ", 4) != 0) {
        const char *err = "ERROR INVALID_COMMAND\n";
        send_exact(client_fd, err, strlen(err));
        return;
    }

    char *topic_query = req_line + 4;
    while (*topic_query == ' ') topic_query++; /* Trim leading spaces */

    /* Basic security sanitization against directory traversal */
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

    /* Send header: OK <file_count> <total_bytes>\n */
    char resp_header[MAX_LINE_LEN];
    snprintf(resp_header, sizeof(resp_header), "OK %u %lu\n",
             topic->file_count, (unsigned long)topic->total_topic_bytes);
    if (send_exact(client_fd, resp_header, strlen(resp_header)) < 0) {
        return;
    }

    /* Stream each file: FILE <filename> <size>\n followed by exact raw binary bytes */
    char chunk[BUFFER_SIZE];
    for (uint32_t i = 0; i < topic->file_count; i++) {
        const FileRecord *f = &topic->files[i];

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

        uint64_t remaining = f->file_size;
        while (remaining > 0) {
            size_t to_read = (remaining > sizeof(chunk)) ? sizeof(chunk) : remaining;
            ssize_t bytes_read = read(file_fd, chunk, to_read);
            if (bytes_read <= 0) {
                break;
            }

            if (send_exact(client_fd, chunk, (size_t)bytes_read) < 0) {
                close(file_fd);
                return;
            }
            remaining -= (uint64_t)bytes_read;
        }
        close(file_fd);
    }

    /* Send completion delimiter */
    const char *done_msg = "DONE\n";
    send_exact(client_fd, done_msg, strlen(done_msg));
}

static void *client_thread_entry(void *arg) {
    WorkerContext *ctx = (WorkerContext *)arg;
    pthread_detach(pthread_self());

    handle_client_part1(ctx->client_socket, ctx->registry);

    close(ctx->client_socket);
    free(ctx);
    return NULL;
}

static void run_single_threaded(int listen_fd, TopicRegistry *reg) {
    printf("[SERVER] Running in Single-Threaded Sequential mode...\n");
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

        handle_client_part1(client_fd, reg);
        close(client_fd);
    }
}

static void run_multi_threaded(int listen_fd, TopicRegistry *reg) {
    printf("[SERVER] Running in Multi-Threaded Concurrent mode...\n");
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
        ctx->registry = reg;

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread_entry, ctx) != 0) {
            perror("pthread_create");
            close(client_fd);
            free(ctx);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <port> <topics_root_dir> [mode]\n", argv[0]);
        fprintf(stderr, "  mode: multi (default) | single\n");
        return 1;
    }

    int port = atoi(argv[1]);
    const char *topics_root_dir = argv[2];
    int is_multi = 1; /* Default to multi-threaded for grading compatibility */

    if (argc >= 4) {
        if (strcasecmp(argv[3], "single") == 0) {
            is_multi = 0;
        } else if (strcasecmp(argv[3], "multi") == 0) {
            is_multi = 1;
        }
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

    printf("[SERVER] Listening on port %d (Mode: %s)\n", port, is_multi ? "Multi-Threaded" : "Single-Threaded");

    if (is_multi) {
        run_multi_threaded(listen_fd, &g_registry);
    } else {
        run_single_threaded(listen_fd, &g_registry);
    }

    printf("\n[SERVER] Shutting down gracefully...\n");
    close(listen_fd);
    topic_registry_destroy(&g_registry);
    return 0;
}

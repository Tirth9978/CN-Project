#include "common_net.h"
#include "protocol.h"
#include "task_queue.h"
#include <fcntl.h>
#include <pthread.h>

#define DEFAULT_CONNECTIONS 4
#define JOURNAL_MAGIC 0x4A4F5552 /* 'JOUR' */

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;
    uint32_t session_id;
    char topic[MAX_TOPIC_NAME_LEN];
    uint32_t current_file_index;
    uint64_t current_byte_offset;
    uint8_t completed_mask[MAX_FILES_PER_TOPIC / 8];
} ClientJournalRecord;
#pragma pack(pop)

typedef struct {
    int worker_id;
    const char *server_ip;
    int server_port;
    uint32_t session_id;
    const char *final_dir;
    TaskQueue *queue;
    ClientJournalRecord *journal;
    pthread_mutex_t *journal_mutex;
    const char *journal_path;
    uint64_t *total_bytes_transmitted;
    pthread_mutex_t *stats_mutex;
} WorkerArgs;

static void persist_journal(const char *jpath, const ClientJournalRecord *rec) {
    int fd = open(jpath, O_WRONLY | O_CREAT, 0644);
    if (fd >= 0) {
        if (write(fd, rec, sizeof(ClientJournalRecord)) < 0) {
            perror("write journal");
        }
        close(fd);
    }
}

static void *parallel_worker_thread(void *arg) {
    WorkerArgs *w = (WorkerArgs *)arg;
    int sock_fd = -1;
    char chunk[BUFFER_SIZE];

    while (!task_queue_is_finished(w->queue)) {
        FileTask task;
        if (!task_queue_pop(w->queue, &task)) {
            break; /* All tasks completed */
        }

        if (sock_fd < 0) {
            sock_fd = connect_to_server(w->server_ip, w->server_port);
            if (sock_fd < 0) {
                task_queue_push(w->queue, &task);
                usleep(50000);
                continue;
            }
        }

        ProtocolHeader req = {
            .magic = PROTOCOL_MAGIC,
            .msg_type = MSG_TASK_REQUEST,
            .flags = FLAG_SINGLE_TASK,
            .status_code = STATUS_OK,
            .session_id = w->session_id,
            .file_index = task.file_index,
            .byte_offset = task.start_offset,
            .payload_len = 0
        };
        ProtocolHeader net_req = req;
        header_to_net(&net_req);

        if (send_exact(sock_fd, &net_req, sizeof(net_req)) < 0) {
            close(sock_fd);
            sock_fd = -1;
            task_queue_push(w->queue, &task);
            continue;
        }

        pthread_mutex_lock(w->stats_mutex);
        *(w->total_bytes_transmitted) += sizeof(net_req);
        pthread_mutex_unlock(w->stats_mutex);

        ProtocolHeader ack;
        if (recv_exact(sock_fd, &ack, sizeof(ack)) <= 0) {
            close(sock_fd);
            sock_fd = -1;
            task_queue_push(w->queue, &task);
            continue;
        }
        header_from_net(&ack);

        pthread_mutex_lock(w->stats_mutex);
        *(w->total_bytes_transmitted) += sizeof(ack);
        pthread_mutex_unlock(w->stats_mutex);

        if (ack.status_code != STATUS_OK || ack.msg_type != MSG_RESUME_ACK) {
            close(sock_fd);
            sock_fd = -1;
            task_queue_push(w->queue, &task);
            continue;
        }

        int file_fd = -1;
        int stream_error = 0;
        uint64_t current_offset = task.start_offset;

        while (1) {
            ProtocolHeader hdr;
            if (recv_exact(sock_fd, &hdr, sizeof(hdr)) <= 0) {
                stream_error = 1;
                break;
            }

            pthread_mutex_lock(w->stats_mutex);
            *(w->total_bytes_transmitted) += sizeof(hdr);
            pthread_mutex_unlock(w->stats_mutex);

            header_from_net(&hdr);

            if (hdr.msg_type == MSG_FILE_START) {
                char fname[MAX_FILENAME_LEN];
                if (hdr.payload_len >= sizeof(fname) || recv_exact(sock_fd, fname, hdr.payload_len) <= 0) {
                    stream_error = 1;
                    break;
                }
                fname[hdr.payload_len] = '\0';

                pthread_mutex_lock(w->stats_mutex);
                *(w->total_bytes_transmitted) += hdr.payload_len;
                pthread_mutex_unlock(w->stats_mutex);

                char out_path[PATH_MAX];
                size_t dlen = strlen(w->final_dir);
                size_t flen = strlen(fname);
                if (dlen + 1 + flen < sizeof(out_path)) {
                    memcpy(out_path, w->final_dir, dlen);
                    out_path[dlen] = '/';
                    memcpy(out_path + dlen + 1, fname, flen);
                    out_path[dlen + 1 + flen] = '\0';
                }

                if (hdr.byte_offset == 0) {
                    file_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                } else {
                    file_fd = open(out_path, O_WRONLY | O_CREAT, 0644);
                    if (file_fd >= 0) {
                        lseek(file_fd, (off_t)hdr.byte_offset, SEEK_SET);
                    }
                }

                if (file_fd < 0) {
                    perror("open destination file");
                    stream_error = 1;
                    break;
                }

            } else if (hdr.msg_type == MSG_DATA_PAYLOAD) {
                uint64_t remaining_bytes = hdr.payload_len;
                while (remaining_bytes > 0) {
                    size_t to_recv = (remaining_bytes > sizeof(chunk)) ? sizeof(chunk) : remaining_bytes;
                    ssize_t n = recv_exact(sock_fd, chunk, to_recv);
                    if (n <= 0) {
                        stream_error = 1;
                        break;
                    }

                    pthread_mutex_lock(w->stats_mutex);
                    *(w->total_bytes_transmitted) += (size_t)n;
                    pthread_mutex_unlock(w->stats_mutex);

                    if (file_fd >= 0) {
                        if (write(file_fd, chunk, (size_t)n) != n) {
                            perror("write file payload");
                            stream_error = 1;
                            break;
                        }
                    }
                    remaining_bytes -= (uint64_t)n;
                    current_offset += (uint64_t)n;
                }

                if (stream_error) break;

            } else if (hdr.msg_type == MSG_FILE_COMPLETE) {
                if (file_fd >= 0) {
                    close(file_fd);
                    file_fd = -1;
                }

                pthread_mutex_lock(w->journal_mutex);
                w->journal->completed_mask[task.file_index / 8] |= (1 << (task.file_index % 8));
                persist_journal(w->journal_path, w->journal);
                pthread_mutex_unlock(w->journal_mutex);

                task_queue_mark_completed(w->queue);

                pthread_mutex_lock(w->stats_mutex);
                static int s_done_count = 0;
                s_done_count++;
                if (s_done_count % 300 == 0 || s_done_count == w->queue->total_tasks) {
                    printf("[CLIENT] Progress: %d / %d files completed (%.1f MB)...\n",
                           s_done_count, w->queue->total_tasks,
                           (double)(*(w->total_bytes_transmitted)) / (1024.0 * 1024.0));
                    fflush(stdout);
                }
                pthread_mutex_unlock(w->stats_mutex);
                break; /* Successfully finished this file task! */

            } else {
                stream_error = 1;
                break;
            }
        }

        if (file_fd >= 0) {
            close(file_fd);
            file_fd = -1;
        }

        if (stream_error) {
            close(sock_fd);
            sock_fd = -1;
            task.start_offset = current_offset;
            printf("[CLIENT] [Worker %d] Stream interrupted at FileIdx=%u, Offset=%lu. Reconnecting to resume...\n",
                   w->worker_id, task.file_index, (unsigned long)task.start_offset);
            fflush(stdout);
            task_queue_push(w->queue, &task);
            usleep(20000);
        }
    }

    if (sock_fd >= 0) {
        close(sock_fd);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir> [num_connections]\n", argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];
    int num_connections = DEFAULT_CONNECTIONS;

    if (argc >= 6) {
        num_connections = atoi(argv[5]);
        if (num_connections < 1) num_connections = 1;
        if (num_connections > 32) num_connections = 32;
    }

    setup_signal_handlers();

    char final_dir[PATH_MAX];
    char journal_path[PATH_MAX];

    size_t outdir_len = strlen(output_dir);
    size_t top_len = strlen(topic);

    if (outdir_len + 1 + top_len >= sizeof(final_dir)) {
        fprintf(stderr, "[CLIENT] Directory path exceeds PATH_MAX\n");
        return 1;
    }

    memcpy(final_dir, output_dir, outdir_len);
    final_dir[outdir_len] = '/';
    memcpy(final_dir + outdir_len + 1, topic, top_len);
    final_dir[outdir_len + 1 + top_len] = '\0';

    snprintf(journal_path, sizeof(journal_path), "%s/.%s.journal", output_dir, topic);

    if (mkdir_p(final_dir, 0755) != 0) {
        perror("mkdir_p final_dir");
        return 1;
    }

    printf("[CLIENT] Initializing Master Connection to %s:%d (Parallel Streams: %d)...\n",
           server_ip, server_port, num_connections);

    int master_fd = connect_to_server(server_ip, server_port);
    if (master_fd < 0) {
        fprintf(stderr, "[CLIENT] Failed to connect master socket\n");
        return 1;
    }

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* Initiate Session */
    ProtocolHeader init_hdr = {
        .magic = PROTOCOL_MAGIC,
        .msg_type = MSG_INIT_REQUEST,
        .flags = FLAG_SINGLE_TASK,
        .status_code = STATUS_OK,
        .session_id = 0,
        .file_index = 0,
        .byte_offset = 0,
        .payload_len = strlen(topic)
    };
    ProtocolHeader net_init = init_hdr;
    header_to_net(&net_init);

    if (send_exact(master_fd, &net_init, sizeof(net_init)) < 0 ||
        send_exact(master_fd, topic, init_hdr.payload_len) < 0) {
        perror("send init header");
        close(master_fd);
        return 1;
    }

    uint64_t total_bytes_transmitted = sizeof(init_hdr) + init_hdr.payload_len;

    ProtocolHeader manifest;
    if (recv_exact(master_fd, &manifest, sizeof(manifest)) <= 0) {
        perror("recv manifest");
        close(master_fd);
        return 1;
    }
    total_bytes_transmitted += sizeof(manifest);
    header_from_net(&manifest);

    if (manifest.status_code != STATUS_OK || manifest.msg_type != MSG_SESSION_MANIFEST) {
        fprintf(stderr, "[CLIENT] Session init failed, code: %d\n", manifest.status_code);
        close(master_fd);
        return 1;
    }

    uint32_t session_id = manifest.session_id;
    uint32_t total_files = manifest.file_index;
    uint64_t useful_bytes = manifest.payload_len;
    close(master_fd);

    printf("[CLIENT] Session %u established: %u files, %.2f MB\n",
           session_id, total_files, (double)useful_bytes / (1024.0 * 1024.0));

    ClientJournalRecord journal;
    memset(&journal, 0, sizeof(journal));
    journal.magic = JOURNAL_MAGIC;
    journal.session_id = session_id;
    snprintf(journal.topic, sizeof(journal.topic), "%s", topic);
    persist_journal(journal_path, &journal);

    TaskQueue queue;
    task_queue_init(&queue, total_files);

    /* Populate task queue */
    for (uint32_t i = 0; i < total_files; i++) {
        FileTask task;
        task.file_index = i;
        task.filename[0] = '\0';
        task.file_size = 0;
        task.start_offset = 0;
        task_queue_push(&queue, &task);
    }

    pthread_mutex_t journal_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

    pthread_t workers[32];
    WorkerArgs wargs[32];

    for (int i = 0; i < num_connections; i++) {
        wargs[i].worker_id = i + 1;
        wargs[i].server_ip = server_ip;
        wargs[i].server_port = server_port;
        wargs[i].session_id = session_id;
        wargs[i].final_dir = final_dir;
        wargs[i].queue = &queue;
        wargs[i].journal = &journal;
        wargs[i].journal_mutex = &journal_mutex;
        wargs[i].journal_path = journal_path;
        wargs[i].total_bytes_transmitted = &total_bytes_transmitted;
        wargs[i].stats_mutex = &stats_mutex;

        pthread_create(&workers[i], NULL, parallel_worker_thread, &wargs[i]);
    }

    /* Wait for all parallel worker streams */
    for (int i = 0; i < num_connections; i++) {
        pthread_join(workers[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    unlink(journal_path); /* Clean journal */
    task_queue_destroy(&queue);

    double elapsed_sec = (t_end.tv_sec - t_start.tv_sec) +
                         (t_end.tv_nsec - t_start.tv_nsec) / 1000000000.0;
    if (elapsed_sec <= 0.0) elapsed_sec = 0.000001;

    uint64_t redundant_bytes = (total_bytes_transmitted > useful_bytes) ? (total_bytes_transmitted - useful_bytes) : 0;
    double overhead_pct = (useful_bytes > 0) ? ((double)redundant_bytes / (double)useful_bytes * 100.0) : 0.0;
    double throughput_mbps = ((double)useful_bytes * 8.0) / (elapsed_sec * 1000000.0);
    double throughput_mbs = ((double)useful_bytes / (1024.0 * 1024.0)) / elapsed_sec;

    printf("\n====================================================\n");
    printf(" Part 2 Case 2 Enhanced (Parallel TCP) Complete!\n");
    printf(" Topic:                %s\n", topic);
    printf(" Parallel Connections: %d\n", num_connections);
    printf(" Files Received:       %u / %u\n", total_files, total_files);
    printf(" Useful Delivered:     %lu bytes (%.2f MB)\n", (unsigned long)useful_bytes, (double)useful_bytes / (1024.0 * 1024.0));
    printf(" Total Transmitted:    %lu bytes (%.2f MB)\n", (unsigned long)total_bytes_transmitted, (double)total_bytes_transmitted / (1024.0 * 1024.0));
    printf(" Redundant Retransmit: %lu bytes (%.2f MB)\n", (unsigned long)redundant_bytes, (double)redundant_bytes / (1024.0 * 1024.0));
    printf(" Protocol Overhead:    %.2f %%\n", overhead_pct);
    printf(" Elapsed Duration:     %.4f seconds\n", elapsed_sec);
    printf(" Effective Throughput: %.2f MB/s (%.2f Mbps)\n", throughput_mbs, throughput_mbps);
    printf(" Output Directory:     %s\n", final_dir);
    printf("====================================================\n");

    return 0;
}

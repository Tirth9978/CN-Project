#include "common_net.h"
#include "protocol.h"
#include <fcntl.h>

#define MAX_RETRIES 30
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

static void persist_journal(const char *jpath, const ClientJournalRecord *rec) {
    int fd = open(jpath, O_WRONLY | O_CREAT | O_SYNC, 0644);
    if (fd >= 0) {
        if (write(fd, rec, sizeof(ClientJournalRecord)) < 0) {
            perror("write journal");
        }
        fsync(fd);
        close(fd);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>\n", argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];

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

    ClientJournalRecord journal;
    memset(&journal, 0, sizeof(journal));

    /* Check for existing checkpoint journal */
    int jfd = open(journal_path, O_RDONLY);
    int has_journal = 0;
    if (jfd >= 0) {
        if (read(jfd, &journal, sizeof(journal)) == sizeof(journal) &&
            journal.magic == JOURNAL_MAGIC &&
            strcmp(journal.topic, topic) == 0) {
            has_journal = 1;
            printf("[CLIENT] Located existing journal for topic '%s': SessionID=%u, FileIdx=%u\n",
                   topic, journal.session_id, journal.current_file_index);
        }
        close(jfd);
    }

    if (!has_journal) {
        journal.magic = JOURNAL_MAGIC;
        journal.session_id = 0;
        snprintf(journal.topic, sizeof(journal.topic), "%s", topic);
        journal.current_file_index = 0;
        journal.current_byte_offset = 0;
        memset(journal.completed_mask, 0, sizeof(journal.completed_mask));
    }

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    uint64_t total_socket_bytes = 0;
    uint64_t useful_bytes = 0;
    uint32_t total_files = 0;
    int attempt = 0;
    int transfer_complete = 0;
    char chunk[BUFFER_SIZE];

    while (attempt < MAX_RETRIES && !transfer_complete) {
        attempt++;
        if (attempt > 1) {
            printf("[CLIENT] [Attempt %d] Reconnecting and requesting resume from FileIdx=%u\n",
                   attempt, journal.current_file_index);
            unsigned int backoff_ms = (attempt < 6) ? (50000 * (1 << attempt)) : 1000000;
            usleep(backoff_ms);
        }

        int sock_fd = connect_to_server(server_ip, server_port);
        if (sock_fd < 0) {
            fprintf(stderr, "[CLIENT] Failed to connect on attempt %d\n", attempt);
            continue;
        }

        if (journal.session_id == 0) {
            /* Initial handshake */
            ProtocolHeader init_hdr = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_INIT_REQUEST,
                .flags = 0,
                .status_code = STATUS_OK,
                .session_id = 0,
                .file_index = 0,
                .byte_offset = 0,
                .payload_len = strlen(topic)
            };
            ProtocolHeader net_init = init_hdr;
            header_to_net(&net_init);

            if (send_exact(sock_fd, &net_init, sizeof(net_init)) < 0 ||
                send_exact(sock_fd, topic, init_hdr.payload_len) < 0) {
                close(sock_fd);
                continue;
            }
            total_socket_bytes += sizeof(init_hdr) + init_hdr.payload_len;

            ProtocolHeader manifest;
            if (recv_exact(sock_fd, &manifest, sizeof(manifest)) <= 0) {
                close(sock_fd);
                continue;
            }
            total_socket_bytes += sizeof(manifest);
            header_from_net(&manifest);

            if (manifest.status_code != STATUS_OK || manifest.msg_type != MSG_SESSION_MANIFEST) {
                fprintf(stderr, "[CLIENT] Session init failed, status: %d\n", manifest.status_code);
                close(sock_fd);
                break;
            }

            journal.session_id = manifest.session_id;
            total_files = manifest.file_index;
            useful_bytes = manifest.payload_len;
            persist_journal(journal_path, &journal);

            printf("[CLIENT] Initialized Session %u: %u files, %.2f MB\n",
                   journal.session_id, total_files, (double)useful_bytes / (1024.0 * 1024.0));
        } else {
            /* Resume handshake */
            ProtocolHeader resume_req = {
                .magic = PROTOCOL_MAGIC,
                .msg_type = MSG_RESUME_REQUEST,
                .flags = 0,
                .status_code = STATUS_OK,
                .session_id = journal.session_id,
                .file_index = journal.current_file_index,
                .byte_offset = journal.current_byte_offset,
                .payload_len = 0
            };
            ProtocolHeader net_req = resume_req;
            header_to_net(&net_req);

            if (send_exact(sock_fd, &net_req, sizeof(net_req)) < 0) {
                close(sock_fd);
                continue;
            }
            total_socket_bytes += sizeof(resume_req);

            ProtocolHeader ack;
            if (recv_exact(sock_fd, &ack, sizeof(ack)) <= 0) {
                close(sock_fd);
                continue;
            }
            total_socket_bytes += sizeof(ack);
            header_from_net(&ack);

            if (ack.status_code != STATUS_OK || ack.msg_type != MSG_RESUME_ACK) {
                fprintf(stderr, "[CLIENT] Resume rejected by server (Status: %d). Resetting session.\n", ack.status_code);
                journal.session_id = 0;
                unlink(journal_path);
                close(sock_fd);
                continue;
            }
            printf("[CLIENT] Server confirmed resume at FileIdx=%u\n",
                   ack.file_index);
        }

        int cur_file_fd = -1;
        int stream_broken = 0;

        while (!stream_broken) {
            ProtocolHeader hdr;
            if (recv_exact(sock_fd, &hdr, sizeof(hdr)) <= 0) {
                stream_broken = 1;
                break;
            }
            total_socket_bytes += sizeof(hdr);
            header_from_net(&hdr);

            if (hdr.magic != PROTOCOL_MAGIC) {
                fprintf(stderr, "[CLIENT] Corrupt magic received: 0x%08X\n", hdr.magic);
                stream_broken = 1;
                break;
            }

            if (hdr.msg_type == MSG_FILE_START) {
                if (cur_file_fd >= 0) {
                    close(cur_file_fd);
                    cur_file_fd = -1;
                }

                char fname[MAX_FILENAME_LEN];
                if (hdr.payload_len >= sizeof(fname) ||
                    recv_exact(sock_fd, fname, hdr.payload_len) <= 0) {
                    stream_broken = 1;
                    break;
                }
                fname[hdr.payload_len] = '\0';
                total_socket_bytes += hdr.payload_len;

                char out_path[PATH_MAX];
                size_t fdir_len = strlen(final_dir);
                size_t fn_len = strlen(fname);
                if (fdir_len + 1 + fn_len >= sizeof(out_path)) {
                    stream_broken = 1;
                    break;
                }
                memcpy(out_path, final_dir, fdir_len);
                out_path[fdir_len] = '/';
                memcpy(out_path + fdir_len + 1, fname, fn_len);
                out_path[fdir_len + 1 + fn_len] = '\0';

                journal.current_file_index = hdr.file_index;
                journal.current_byte_offset = hdr.byte_offset;

                if (hdr.byte_offset == 0) {
                    cur_file_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                } else {
                    cur_file_fd = open(out_path, O_WRONLY | O_CREAT, 0644);
                    if (cur_file_fd >= 0) {
                        lseek(cur_file_fd, (off_t)hdr.byte_offset, SEEK_SET);
                    }
                }

                if (cur_file_fd < 0) {
                    perror("open destination file");
                    stream_broken = 1;
                    break;
                }
                persist_journal(journal_path, &journal);

            } else if (hdr.msg_type == MSG_DATA_PAYLOAD) {
                uint64_t remaining_chunk = hdr.payload_len;
                while (remaining_chunk > 0) {
                    size_t to_recv = (remaining_chunk > sizeof(chunk)) ? sizeof(chunk) : remaining_chunk;
                    ssize_t n = recv_exact(sock_fd, chunk, to_recv);
                    if (n <= 0) {
                        stream_broken = 1;
                        break;
                    }
                    total_socket_bytes += (size_t)n;

                    if (cur_file_fd >= 0) {
                        if (write(cur_file_fd, chunk, (size_t)n) != n) {
                            perror("write data payload");
                            stream_broken = 1;
                            break;
                        }
                    }
                    remaining_chunk -= (uint64_t)n;
                    journal.current_byte_offset += (uint64_t)n;
                }

                if (stream_broken) break;
                persist_journal(journal_path, &journal);

            } else if (hdr.msg_type == MSG_FILE_COMPLETE) {
                if (cur_file_fd >= 0) {
                    close(cur_file_fd);
                    cur_file_fd = -1;
                }
                journal.completed_mask[journal.current_file_index / 8] |= (1 << (journal.current_file_index % 8));
                journal.current_file_index++;
                journal.current_byte_offset = 0;
                persist_journal(journal_path, &journal);

            } else if (hdr.msg_type == MSG_TOPIC_COMPLETE) {
                if (cur_file_fd >= 0) {
                    close(cur_file_fd);
                    cur_file_fd = -1;
                }
                transfer_complete = 1;
                unlink(journal_path); /* Remove journal on clean completion */
                break;
            } else if (hdr.msg_type == MSG_ERROR) {
                fprintf(stderr, "[CLIENT] Server reported error code %d\n", hdr.status_code);
                stream_broken = 1;
                break;
            }
        }

        if (cur_file_fd >= 0) {
            close(cur_file_fd);
            cur_file_fd = -1;
        }
        close(sock_fd);
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    if (!transfer_complete) {
        fprintf(stderr, "[CLIENT] Failed to complete transfer for '%s' after %d attempts.\n", topic, attempt);
        return 1;
    }

    double elapsed_sec = (t_end.tv_sec - t_start.tv_sec) +
                         (t_end.tv_nsec - t_start.tv_nsec) / 1000000000.0;
    if (elapsed_sec <= 0.0) elapsed_sec = 0.000001;

    uint64_t redundant_bytes = (total_socket_bytes > useful_bytes) ? (total_socket_bytes - useful_bytes) : 0;
    double overhead_pct = (useful_bytes > 0) ? ((double)redundant_bytes / (double)useful_bytes * 100.0) : 0.0;
    double throughput_mbps = ((double)useful_bytes * 8.0) / (elapsed_sec * 1000000.0);

    printf("\n====================================================\n");
    printf(" Part 2 Case 2 (Stateful Resume) Transfer Complete!\n");
    printf(" Topic:                %s\n", topic);
    printf(" Total Attempts:       %d\n", attempt);
    printf(" Useful Delivered:     %lu bytes (%.2f MB)\n", (unsigned long)useful_bytes, (double)useful_bytes / (1024.0 * 1024.0));
    printf(" Total Transmitted:    %lu bytes (%.2f MB)\n", (unsigned long)total_socket_bytes, (double)total_socket_bytes / (1024.0 * 1024.0));
    printf(" Redundant Retransmit: %lu bytes (%.2f MB)\n", (unsigned long)redundant_bytes, (double)redundant_bytes / (1024.0 * 1024.0));
    printf(" Protocol Overhead:    %.2f %%\n", overhead_pct);
    printf(" Elapsed Duration:     %.4f seconds\n", elapsed_sec);
    printf(" Effective Throughput: %.2f Mbps\n", throughput_mbps);
    printf(" Output Directory:     %s\n", final_dir);
    printf("====================================================\n");

    return 0;
}

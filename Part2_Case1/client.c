#include "common_net.h"
#include <fcntl.h>

#define MAX_RETRIES 50

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
    char staging_dir[PATH_MAX];

    size_t outdir_len = strlen(output_dir);
    size_t top_len = strlen(topic);

    if (outdir_len + 1 + top_len >= sizeof(final_dir)) {
        fprintf(stderr, "[CLIENT] Directory path exceeds PATH_MAX\n");
        return 1;
    }

    /* final_dir: output_dir/topic */
    memcpy(final_dir, output_dir, outdir_len);
    final_dir[outdir_len] = '/';
    memcpy(final_dir + outdir_len + 1, topic, top_len);
    final_dir[outdir_len + 1 + top_len] = '\0';

    /* staging_dir: output_dir/.<topic>_staging */
    snprintf(staging_dir, sizeof(staging_dir), "%s/.%s_staging", output_dir, topic);

    mkdir_p(output_dir, 0755);

    printf("[CLIENT] Starting Stateless Transfer for topic '%s'...\n", topic);

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    uint64_t total_socket_bytes = 0;
    uint64_t useful_bytes = 0;
    int attempt = 0;
    int success = 0;
    char chunk[BUFFER_SIZE];

    while (attempt < MAX_RETRIES && !success) {
        attempt++;
        if (attempt > 1) {
            printf("[CLIENT] [Attempt %d] Connection severed. Discarding staging and restarting from scratch...\n", attempt);
            unsigned int backoff_ms = (attempt < 6) ? (50000 * (1 << attempt)) : 1000000;
            usleep(backoff_ms);
        }

        /* Wipe and re-create staging folder */
        remove_directory_recursive(staging_dir);
        if (mkdir_p(staging_dir, 0755) != 0) {
            perror("mkdir_p staging_dir");
            continue;
        }

        int sock_fd = -1;
        for (int conn_try = 0; conn_try < 10 && sock_fd < 0; conn_try++) {
            sock_fd = connect_to_server(server_ip, server_port);
            if (sock_fd < 0) {
                usleep(300000); /* Wait 300ms if server is binding/initializing */
            }
        }
        if (sock_fd < 0) {
            fprintf(stderr, "[CLIENT] Failed to connect to %s:%d\n", server_ip, server_port);
            continue;
        }

        /* Send request: GET <topic>\n */
        char req[MAX_LINE_LEN];
        snprintf(req, sizeof(req), "GET %s\n", topic);
        size_t req_len = strlen(req);
        if (send_exact(sock_fd, req, req_len) < 0) {
            close(sock_fd);
            continue;
        }
        total_socket_bytes += req_len;

        /* Read response: OK <file_count> <total_bytes>\n */
        char line[MAX_LINE_LEN];
        ssize_t hdr_len = recv_line(sock_fd, line, sizeof(line));
        if (hdr_len <= 0) {
            close(sock_fd);
            continue;
        }
        total_socket_bytes += (uint64_t)hdr_len;

        if (strncmp(line, "OK ", 3) != 0) {
            fprintf(stderr, "[CLIENT] Server error: %s\n", line);
            close(sock_fd);
            break;
        }

        uint32_t total_files = 0;
        uint64_t topic_bytes = 0;
        if (sscanf(line + 3, "%u %lu", &total_files, &topic_bytes) != 2) {
            close(sock_fd);
            continue;
        }

        useful_bytes = topic_bytes;
        uint32_t files_received = 0;
        int attempt_failed = 0;

        while (files_received < total_files) {
            ssize_t l_len = recv_line(sock_fd, line, sizeof(line));
            if (l_len <= 0) {
                attempt_failed = 1;
                break;
            }
            total_socket_bytes += (uint64_t)l_len;

            if (strcmp(line, "DONE") == 0) {
                break;
            }

            if (strncmp(line, "FILE ", 5) != 0) {
                attempt_failed = 1;
                break;
            }

            char filename[MAX_FILENAME_LEN];
            uint64_t file_size = 0;
            if (sscanf(line + 5, "%255s %lu", filename, &file_size) != 2) {
                attempt_failed = 1;
                break;
            }

            char out_path[PATH_MAX];
            size_t sdir_len = strlen(staging_dir);
            size_t fn_len = strlen(filename);
            if (sdir_len + 1 + fn_len >= sizeof(out_path)) {
                attempt_failed = 1;
                break;
            }
            memcpy(out_path, staging_dir, sdir_len);
            out_path[sdir_len] = '/';
            memcpy(out_path + sdir_len + 1, filename, fn_len);
            out_path[sdir_len + 1 + fn_len] = '\0';

            int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (out_fd < 0) {
                perror("open staging file");
                attempt_failed = 1;
                break;
            }

            uint64_t remaining = file_size;
            while (remaining > 0) {
                size_t to_read = (remaining > sizeof(chunk)) ? sizeof(chunk) : remaining;
                ssize_t n = recv_exact(sock_fd, chunk, to_read);
                if (n <= 0) {
                    attempt_failed = 1;
                    break;
                }
                total_socket_bytes += (uint64_t)n;

                if (write(out_fd, chunk, (size_t)n) != n) {
                    perror("write staging file");
                    attempt_failed = 1;
                    break;
                }
                remaining -= (uint64_t)n;
            }
            close(out_fd);

            if (attempt_failed) {
                break;
            }
            files_received++;
        }

        if (!attempt_failed && files_received == total_files) {
            ssize_t done_len = recv_line(sock_fd, line, sizeof(line));
            if (done_len > 0) {
                total_socket_bytes += (uint64_t)done_len;
            }
            close(sock_fd);

            /* Atomically replace final directory */
            remove_directory_recursive(final_dir);
            if (rename(staging_dir, final_dir) != 0) {
                perror("rename staging to final directory");
            }
            success = 1;
            break;
        }

        close(sock_fd);
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    if (!success) {
        fprintf(stderr, "[CLIENT] Failed to transfer topic '%s' after %d attempts.\n", topic, attempt);
        remove_directory_recursive(staging_dir);
        return 1;
    }

    double elapsed_sec = (t_end.tv_sec - t_start.tv_sec) +
                         (t_end.tv_nsec - t_start.tv_nsec) / 1000000000.0;
    if (elapsed_sec <= 0.0) elapsed_sec = 0.000001;

    uint64_t redundant_bytes = (total_socket_bytes > useful_bytes) ? (total_socket_bytes - useful_bytes) : 0;
    double overhead_pct = (useful_bytes > 0) ? ((double)redundant_bytes / (double)useful_bytes * 100.0) : 0.0;
    double throughput_mbps = ((double)useful_bytes * 8.0) / (elapsed_sec * 1000000.0);

    printf("\n====================================================\n");
    printf(" Part 2 Case 1 (Stateless) Transfer Complete!\n");
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

#include "common_net.h"
#include <fcntl.h>

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

    /* Target directory: output_dir/topic/ */
    char target_dir[PATH_MAX];
    size_t outdir_len = strlen(output_dir);
    size_t top_len = strlen(topic);
    if (outdir_len + 1 + top_len >= sizeof(target_dir)) {
        fprintf(stderr, "[CLIENT] Target directory path exceeds PATH_MAX\n");
        return 1;
    }
    memcpy(target_dir, output_dir, outdir_len);
    target_dir[outdir_len] = '/';
    memcpy(target_dir + outdir_len + 1, topic, top_len);
    target_dir[outdir_len + 1 + top_len] = '\0';

    if (mkdir_p(target_dir, 0755) != 0) {
        perror("mkdir_p target directory");
        return 1;
    }

    printf("[CLIENT] Connecting to %s:%d...\n", server_ip, server_port);
    int sock_fd = connect_to_server(server_ip, server_port);
    if (sock_fd < 0) {
        fprintf(stderr, "[CLIENT] Failed to connect to server at %s:%d\n", server_ip, server_port);
        return 1;
    }

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* Send request: GET <topic>\n */
    char req[MAX_LINE_LEN];
    snprintf(req, sizeof(req), "GET %s\n", topic);
    if (send_exact(sock_fd, req, strlen(req)) < 0) {
        perror("send GET request");
        close(sock_fd);
        return 1;
    }

    /* Read response header */
    char line[MAX_LINE_LEN];
    if (recv_line(sock_fd, line, sizeof(line)) <= 0) {
        fprintf(stderr, "[CLIENT] Failed to read response header from server\n");
        close(sock_fd);
        return 1;
    }

    if (strncmp(line, "OK ", 3) != 0) {
        fprintf(stderr, "[CLIENT] Server error: %s\n", line);
        close(sock_fd);
        return 1;
    }

    uint32_t total_files = 0;
    uint64_t total_expected_bytes = 0;
    if (sscanf(line + 3, "%u %lu", &total_files, &total_expected_bytes) != 2) {
        fprintf(stderr, "[CLIENT] Malformed OK response: %s\n", line);
        close(sock_fd);
        return 1;
    }

    printf("[CLIENT] Topic '%s' confirmed: %u files, %.2f MB\n",
           topic, total_files, (double)total_expected_bytes / (1024.0 * 1024.0));

    uint32_t files_received = 0;
    uint64_t bytes_received = 0;
    char chunk[BUFFER_SIZE];

    while (files_received < total_files) {
        ssize_t hdr_len = recv_line(sock_fd, line, sizeof(line));
        if (hdr_len <= 0) {
            fprintf(stderr, "[CLIENT] Connection dropped while waiting for file header\n");
            break;
        }

        if (strcmp(line, "DONE") == 0) {
            break;
        }

        if (strncmp(line, "FILE ", 5) != 0) {
            fprintf(stderr, "[CLIENT] Unexpected protocol token: %s\n", line);
            break;
        }

        char filename[MAX_FILENAME_LEN];
        uint64_t file_size = 0;
        if (sscanf(line + 5, "%255s %lu", filename, &file_size) != 2) {
            fprintf(stderr, "[CLIENT] Malformed FILE line: %s\n", line);
            break;
        }

        char out_path[PATH_MAX];
        size_t tdir_len = strlen(target_dir);
        size_t fname_len = strlen(filename);
        if (tdir_len + 1 + fname_len >= sizeof(out_path)) {
            fprintf(stderr, "[CLIENT] Output file path exceeds PATH_MAX\n");
            break;
        }
        memcpy(out_path, target_dir, tdir_len);
        out_path[tdir_len] = '/';
        memcpy(out_path + tdir_len + 1, filename, fname_len);
        out_path[tdir_len + 1 + fname_len] = '\0';

        int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (out_fd < 0) {
            perror("open local output file");
            break;
        }

        uint64_t remaining = file_size;
        int error = 0;
        while (remaining > 0) {
            size_t to_read = (remaining > sizeof(chunk)) ? sizeof(chunk) : remaining;
            ssize_t n = recv_exact(sock_fd, chunk, to_read);
            if (n <= 0) {
                fprintf(stderr, "[CLIENT] Connection severed while receiving %s\n", filename);
                error = 1;
                break;
            }

            if (write(out_fd, chunk, (size_t)n) != n) {
                perror("write to file");
                error = 1;
                break;
            }
            remaining -= (uint64_t)n;
            bytes_received += (uint64_t)n;
        }
        close(out_fd);

        if (error) {
            break;
        }
        files_received++;
    }

    /* Read trailing DONE delimiter if not encountered already */
    if (files_received == total_files) {
        if (recv_line(sock_fd, line, sizeof(line)) > 0 && strcmp(line, "DONE") != 0) {
            fprintf(stderr, "[CLIENT] Warning: Expected DONE, got: %s\n", line);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    close(sock_fd);

    double elapsed_sec = (t_end.tv_sec - t_start.tv_sec) +
                         (t_end.tv_nsec - t_start.tv_nsec) / 1000000000.0;
    if (elapsed_sec <= 0.0) elapsed_sec = 0.000001;

    double mb_received = (double)bytes_received / (1024.0 * 1024.0);
    double mb_per_sec = mb_received / elapsed_sec;
    double mbps = (mb_received * 8.0) / elapsed_sec;

    printf("\n====================================================\n");
    printf(" Transfer Summary: Topic '%s'\n", topic);
    printf(" Files Received:      %u / %u\n", files_received, total_files);
    printf(" Bytes Transferred:   %lu bytes (%.2f MB)\n", (unsigned long)bytes_received, mb_received);
    printf(" Elapsed Duration:    %.4f seconds\n", elapsed_sec);
    printf(" Throughput Rate:     %.2f MB/s (%.2f Mbps)\n", mb_per_sec, mbps);
    printf(" Output Directory:    %s\n", target_dir);
    printf("====================================================\n");

    return (files_received == total_files) ? 0 : 1;
}

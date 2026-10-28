#ifndef COMMON_NET_H
#define COMMON_NET_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <time.h>
#include <limits.h>

/* 64-bit Network Byte Order Serialization */
#if defined(__linux__) || defined(__GLIBC__)
# include <endian.h>
#else
static inline uint64_t htobe64(uint64_t host) {
    uint32_t high = htonl((uint32_t)(host >> 32));
    uint32_t low  = htonl((uint32_t)(host & 0xFFFFFFFFLL));
    return (((uint64_t)low) << 32) | high;
}
# define be64toh(net) htobe64(net)
#endif

#define MAX_FILENAME_LEN 256
#define MAX_TOPIC_NAME_LEN 64
#define BUFFER_SIZE 65536
#define MAX_LINE_LEN 1024
#define DEFAULT_TIMEOUT_SEC 8

/*
 * send_exact:
 * Repeatedly calls send() until exactly 'count' bytes are transmitted
 * or an unrecoverable error occurs. Handles EINTR gracefully.
 * Returns count on success, or -1 on failure.
 */
ssize_t send_exact(int fd, const void *buf, size_t count);

/*
 * recv_exact:
 * Repeatedly calls recv() until exactly 'count' bytes are received
 * or connection terminates. Handles EINTR gracefully.
 * Returns count on success, 0 on EOF, or -1 on failure.
 */
ssize_t recv_exact(int fd, void *buf, size_t count);

/*
 * recv_line:
 * Reads characters one-by-one from the stream until '\n' is encountered
 * or max_len - 1 is reached. This is critical for protocol control lines
 * to prevent 'buffer bleed' into following binary payloads.
 * Strips '\r' and '\n' and null-terminates the buffer.
 * Returns length of line read, 0 on EOF, or -1 on error.
 */
ssize_t recv_line(int fd, char *buf, size_t max_len);

/*
 * create_server_socket:
 * Creates a TCP socket, configures SO_REUSEADDR, binds to port, and listens.
 * Returns listening socket file descriptor, or -1 on error.
 */
int create_server_socket(int port);

/*
 * connect_to_server:
 * Creates a TCP socket and connects to server_ip at the specified port.
 * Returns connected socket file descriptor, or -1 on error.
 */
int connect_to_server(const char *server_ip, int port);

/*
 * set_socket_reuse:
 * Configures SO_REUSEADDR on the socket to allow immediate port reuse.
 */
int set_socket_reuse(int sock_fd);

/*
 * set_socket_timeouts:
 * Configures SO_RCVTIMEO and SO_SNDTIMEO so network calls don't hang indefinitely.
 */
int set_socket_timeouts(int sock_fd, int timeout_sec);

/*
 * setup_signal_handlers:
 * Sets SIGPIPE to SIG_IGN so abrupt client disconnects return EPIPE
 * instead of terminating the server process.
 */
void setup_signal_handlers(void);

/*
 * mkdir_p:
 * Recursively creates all directories in the given path (like `mkdir -p`).
 * Returns 0 on success, or -1 on error.
 */
int mkdir_p(const char *path, mode_t mode);

#endif /* COMMON_NET_H */

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
#include <netinet/tcp.h>
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

/* Strict byte-stream communication primitives */
ssize_t send_exact(int fd, const void *buf, size_t count);
ssize_t recv_exact(int fd, void *buf, size_t count);

/* Safe line receiver */
ssize_t recv_line(int fd, char *buf, size_t max_len);

/* Socket creation & configuration helpers */
int create_server_socket(int port);
int connect_to_server(const char *server_ip, int port);
int set_socket_reuse(int sock_fd);
int set_socket_timeouts(int sock_fd, int timeout_sec);
void setup_signal_handlers(void);

/* File system & directory helpers */
int mkdir_p(const char *path, mode_t mode);
int remove_directory_recursive(const char *path);

#endif /* COMMON_NET_H */

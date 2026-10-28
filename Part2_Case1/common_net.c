#include "common_net.h"
#include <dirent.h>

ssize_t send_exact(int fd, const void *buf, size_t count) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;

    while (total_sent < count) {
        ssize_t n = send(fd, ptr + total_sent, count - total_sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return (ssize_t)total_sent;
        }
        total_sent += (size_t)n;
    }
    return (ssize_t)total_sent;
}

ssize_t recv_exact(int fd, void *buf, size_t count) {
    size_t total_recv = 0;
    char *ptr = (char *)buf;

    while (total_recv < count) {
        ssize_t n = recv(fd, ptr + total_recv, count - total_recv, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return (ssize_t)total_recv;
        }
        total_recv += (size_t)n;
    }
    return (ssize_t)total_recv;
}

ssize_t recv_line(int fd, char *buf, size_t max_len) {
    if (!buf || max_len == 0) {
        return -1;
    }

    size_t idx = 0;
    while (idx < max_len - 1) {
        char ch;
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            break;
        }

        if (ch == '\n') {
            break;
        }
        if (ch != '\r') {
            buf[idx++] = ch;
        }
    }

    buf[idx] = '\0';
    return (ssize_t)idx;
}

int create_server_socket(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return -1;
    }

    if (set_socket_reuse(listen_fd) < 0) {
        perror("setsockopt SO_REUSEADDR");
        close(listen_fd);
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, 128) < 0) {
        perror("listen");
        close(listen_fd);
        return -1;
    }

    return listen_fd;
}

int connect_to_server(const char *server_ip, int port) {
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo hints, *res, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(server_ip, port_str, &hints, &res) != 0) {
        return -1;
    }

    int sock_fd = -1;
    for (rp = res; rp != NULL; rp = rp->ai_next) {
        sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock_fd < 0) {
            continue;
        }

        if (connect(sock_fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }

        close(sock_fd);
        sock_fd = -1;
    }

    freeaddrinfo(res);
    return sock_fd;
}

int set_socket_reuse(int sock_fd) {
    int opt = 1;
    return setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

int set_socket_timeouts(int sock_fd, int timeout_sec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;

    if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        return -1;
    }
    if (setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        return -1;
    }
    return 0;
}

void setup_signal_handlers(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPIPE, &sa, NULL);
}

int mkdir_p(const char *path, mode_t mode) {
    char tmp[PATH_MAX];
    char *p = NULL;
    size_t len;

    if (!path || !*path) {
        return -1;
    }

    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (len > 0 && tmp[len - 1] == '/') {
        tmp[len - 1] = '\0';
    }

    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, mode) != 0 && errno != EEXIST) {
                return -1;
            }
            *p = '/';
        }
    }

    if (mkdir(tmp, mode) != 0 && errno != EEXIST) {
        return -1;
    }

    return 0;
}

int remove_directory_recursive(const char *path) {
    if (!path || !*path) {
        return -1;
    }

    DIR *dir = opendir(path);
    if (!dir) {
        if (errno == ENOENT) return 0;
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char subpath[PATH_MAX];
        size_t plen = strlen(path);
        size_t elen = strlen(entry->d_name);
        if (plen + 1 + elen >= sizeof(subpath)) {
            continue;
        }

        memcpy(subpath, path, plen);
        subpath[plen] = '/';
        memcpy(subpath + plen + 1, entry->d_name, elen);
        subpath[plen + 1 + elen] = '\0';

        struct stat st;
        if (stat(subpath, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                remove_directory_recursive(subpath);
            } else {
                unlink(subpath);
            }
        }
    }
    closedir(dir);
    return rmdir(path);
}

#include "fault_injector.h"

int fault_injector_should_fail(double p_failure, uint64_t filesize, uint64_t *out_fault_offset, unsigned int *seed) {
    if (p_failure <= 0.0) {
        return 0;
    }
    if (p_failure >= 1.0) {
        *out_fault_offset = (filesize < 1024) ? (filesize / 2) : (filesize / 4);
        return 1;
    }

    double r = (double)rand_r(seed) / (double)RAND_MAX;
    if (r <= p_failure) {
        uint64_t fault_offset = 0;
        if (filesize > 0) {
            double frac = (double)rand_r(seed) / (double)RAND_MAX;
            fault_offset = (uint64_t)(frac * (double)filesize);
        }
        if (filesize < 1024) {
            fault_offset = filesize / 2;
        }
        *out_fault_offset = fault_offset;
        return 1;
    }

    return 0;
}

void execute_abrupt_disconnect(int sock_fd, const char *topic, uint32_t file_idx, uint64_t offset) {
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = 0;

    setsockopt(sock_fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));

    fprintf(stderr, "[FAULT ENGINE] Aborting TCP connection: Topic='%s', FileIdx=%u, ByteOffset=%lu\n",
            topic ? topic : "unknown", file_idx, (unsigned long)offset);

    close(sock_fd);
}

#ifndef FAULT_INJECTOR_H
#define FAULT_INJECTOR_H

#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * fault_injector_should_fail:
 * Evaluates a Bernoulli trial with failure probability p_failure.
 * If true, calculates a failure byte offset in [0, filesize).
 * Returns 1 if fault should occur, 0 otherwise.
 */
int fault_injector_should_fail(double p_failure, uint64_t filesize, uint64_t *out_fault_offset, unsigned int *seed);

/*
 * execute_abrupt_disconnect:
 * Configures SO_LINGER with l_onoff=1 and l_linger=0, then closes the socket.
 * This abruptly resets the TCP stream (sending TCP RST) instead of performing a FIN handshake.
 */
void execute_abrupt_disconnect(int sock_fd, const char *topic, uint32_t file_idx, uint64_t offset);

#endif /* FAULT_INJECTOR_H */

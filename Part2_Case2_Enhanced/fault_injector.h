#ifndef FAULT_INJECTOR_H
#define FAULT_INJECTOR_H

#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

int fault_injector_should_fail(double p_failure, uint64_t filesize, uint64_t *out_fault_offset, unsigned int *seed);
void execute_abrupt_disconnect(int sock_fd, const char *topic, uint32_t file_idx, uint64_t offset);

#endif /* FAULT_INJECTOR_H */

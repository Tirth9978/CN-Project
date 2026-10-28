#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include <stdint.h>
#include <pthread.h>
#include "common_net.h"
#include "protocol.h"

typedef struct {
    uint32_t file_index;
    char filename[MAX_FILENAME_LEN];
    uint64_t file_size;
    uint64_t start_offset;
} FileTask;

typedef struct {
    FileTask tasks[MAX_FILES_PER_TOPIC];
    int head;
    int tail;
    int count;
    int total_tasks;
    int completed_tasks;
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_cond;
} TaskQueue;

void task_queue_init(TaskQueue *q, int total_tasks);
int task_queue_push(TaskQueue *q, const FileTask *task);
int task_queue_pop(TaskQueue *q, FileTask *out_task);
void task_queue_mark_completed(TaskQueue *q);
int task_queue_is_finished(TaskQueue *q);
void task_queue_destroy(TaskQueue *q);

#endif /* TASK_QUEUE_H */

#include "task_queue.h"
#include <string.h>

void task_queue_init(TaskQueue *q, int total_tasks) {
    if (!q) return;
    memset(q, 0, sizeof(TaskQueue));
    q->total_tasks = total_tasks;
    pthread_mutex_init(&q->queue_lock, NULL);
    pthread_cond_init(&q->queue_cond, NULL);
}

int task_queue_push(TaskQueue *q, const FileTask *task) {
    if (!q || !task) return 0;

    pthread_mutex_lock(&q->queue_lock);
    if (q->count >= MAX_FILES_PER_TOPIC) {
        pthread_mutex_unlock(&q->queue_lock);
        return 0;
    }

    q->tasks[q->tail] = *task;
    q->tail = (q->tail + 1) % MAX_FILES_PER_TOPIC;
    q->count++;

    pthread_cond_signal(&q->queue_cond);
    pthread_mutex_unlock(&q->queue_lock);
    return 1;
}

int task_queue_pop(TaskQueue *q, FileTask *out_task) {
    if (!q || !out_task) return 0;

    pthread_mutex_lock(&q->queue_lock);
    while (q->count == 0 && q->completed_tasks < q->total_tasks) {
        pthread_cond_wait(&q->queue_cond, &q->queue_lock);
    }

    if (q->count == 0 && q->completed_tasks >= q->total_tasks) {
        pthread_mutex_unlock(&q->queue_lock);
        return 0; /* All tasks finished */
    }

    *out_task = q->tasks[q->head];
    q->head = (q->head + 1) % MAX_FILES_PER_TOPIC;
    q->count--;

    pthread_mutex_unlock(&q->queue_lock);
    return 1;
}

void task_queue_mark_completed(TaskQueue *q) {
    if (!q) return;

    pthread_mutex_lock(&q->queue_lock);
    q->completed_tasks++;
    pthread_cond_broadcast(&q->queue_cond);
    pthread_mutex_unlock(&q->queue_lock);
}

int task_queue_is_finished(TaskQueue *q) {
    if (!q) return 1;

    pthread_mutex_lock(&q->queue_lock);
    int fin = (q->completed_tasks >= q->total_tasks);
    pthread_mutex_unlock(&q->queue_lock);
    return fin;
}

void task_queue_destroy(TaskQueue *q) {
    if (!q) return;

    pthread_mutex_destroy(&q->queue_lock);
    pthread_cond_destroy(&q->queue_cond);
}

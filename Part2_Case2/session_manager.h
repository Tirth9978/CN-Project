#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include "topic_manager.h"

#define MAX_SESSIONS 256
#define SESSION_TTL_SEC 300

typedef struct {
    uint32_t file_index;
    uint64_t byte_offset;
    uint8_t  is_completed;
    time_t   updated_at;
} FileProgress;

typedef struct {
    uint32_t session_id;
    char topic[MAX_TOPIC_NAME_LEN];
    time_t created_at;
    time_t last_active;
    int is_active;
    uint32_t total_files;
    FileProgress file_progress[MAX_FILES_PER_TOPIC];
    pthread_mutex_t session_mutex;
} ActiveSession;

typedef struct {
    ActiveSession sessions[MAX_SESSIONS];
    uint32_t session_count;
    pthread_mutex_t registry_lock;
} SessionRegistry;

void session_registry_init(SessionRegistry *sreg);
ActiveSession *session_create(SessionRegistry *sreg, const char *topic, uint32_t total_files);
ActiveSession *session_find(SessionRegistry *sreg, uint32_t session_id);
void session_update_progress(ActiveSession *s, uint32_t file_idx, uint64_t offset, uint8_t completed);
void session_destroy_all(SessionRegistry *sreg);

#endif /* SESSION_MANAGER_H */

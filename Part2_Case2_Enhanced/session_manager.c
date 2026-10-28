#include "session_manager.h"
#include <string.h>
#include <stdio.h>

static uint32_t g_next_session_id = 2001;

void session_registry_init(SessionRegistry *sreg) {
    if (!sreg) return;
    memset(sreg, 0, sizeof(SessionRegistry));
    pthread_mutex_init(&sreg->registry_lock, NULL);
}

ActiveSession *session_create(SessionRegistry *sreg, const char *topic, uint32_t total_files) {
    if (!sreg || !topic) return NULL;

    pthread_mutex_lock(&sreg->registry_lock);

    time_t now = time(NULL);
    ActiveSession *target = NULL;

    for (uint32_t i = 0; i < MAX_SESSIONS; i++) {
        if (!sreg->sessions[i].is_active) {
            target = &sreg->sessions[i];
            break;
        } else if (now - sreg->sessions[i].last_active > SESSION_TTL_SEC) {
            pthread_mutex_destroy(&sreg->sessions[i].session_mutex);
            target = &sreg->sessions[i];
            break;
        }
    }

    if (!target) {
        pthread_mutex_unlock(&sreg->registry_lock);
        fprintf(stderr, "[SESSION] Warning: Session registry full (%d sessions)\n", MAX_SESSIONS);
        return NULL;
    }

    memset(target, 0, sizeof(ActiveSession));
    target->session_id = g_next_session_id++;
    snprintf(target->topic, sizeof(target->topic), "%s", topic);
    target->created_at = now;
    target->last_active = now;
    target->is_active = 1;
    target->total_files = total_files;
    pthread_mutex_init(&target->session_mutex, NULL);

    pthread_mutex_unlock(&sreg->registry_lock);
    return target;
}

ActiveSession *session_find(SessionRegistry *sreg, uint32_t session_id) {
    if (!sreg || session_id == 0) return NULL;

    pthread_mutex_lock(&sreg->registry_lock);

    ActiveSession *found = NULL;
    time_t now = time(NULL);

    for (uint32_t i = 0; i < MAX_SESSIONS; i++) {
        if (sreg->sessions[i].is_active && sreg->sessions[i].session_id == session_id) {
            if (now - sreg->sessions[i].last_active <= SESSION_TTL_SEC) {
                found = &sreg->sessions[i];
                found->last_active = now;
            }
            break;
        }
    }

    pthread_mutex_unlock(&sreg->registry_lock);
    return found;
}

void session_update_progress(ActiveSession *s, uint32_t file_idx, uint64_t offset, uint8_t completed) {
    if (!s) return;

    pthread_mutex_lock(&s->session_mutex);
    if (file_idx < MAX_FILES_PER_TOPIC) {
        s->file_progress[file_idx].file_index = file_idx;
        s->file_progress[file_idx].byte_offset = offset;
        s->file_progress[file_idx].is_completed = completed;
        s->file_progress[file_idx].updated_at = time(NULL);
    }
    s->last_active = time(NULL);
    pthread_mutex_unlock(&s->session_mutex);
}

void session_destroy_all(SessionRegistry *sreg) {
    if (!sreg) return;

    pthread_mutex_lock(&sreg->registry_lock);
    for (uint32_t i = 0; i < MAX_SESSIONS; i++) {
        if (sreg->sessions[i].is_active) {
            pthread_mutex_destroy(&sreg->sessions[i].session_mutex);
            sreg->sessions[i].is_active = 0;
        }
    }
    pthread_mutex_unlock(&sreg->registry_lock);
    pthread_mutex_destroy(&sreg->registry_lock);
}

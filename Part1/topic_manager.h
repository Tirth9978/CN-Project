#ifndef TOPIC_MANAGER_H
#define TOPIC_MANAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#define MAX_TOPIC_NAME_LEN 64
#define MAX_FILENAME_LEN 256
#define MAX_TOPICS 32
#define INITIAL_FILES_CAP 8192
#define MAX_FILES_PER_TOPIC 16384

/* Individual file entry metadata */
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char file_path[PATH_MAX];
    uint64_t file_size;
    uint32_t file_index;
} FileRecord;

/* Topic record containing dynamically allocated file array */
typedef struct {
    char topic_name[MAX_TOPIC_NAME_LEN];
    char directory_path[PATH_MAX];
    uint32_t file_count;
    uint32_t capacity;
    uint64_t total_topic_bytes;
    FileRecord *files; /* Dynamically expandable array */
} TopicRecord;

/* Thread-safe global topic catalog */
typedef struct {
    TopicRecord topics[MAX_TOPICS];
    uint32_t topic_count;
    char root_dir[PATH_MAX];
    pthread_rwlock_t rwlock; /* Read-write lock for zero-contention parallel lookups */
} TopicRegistry;

/*
 * topic_registry_init:
 * Dynamically traverses root_dir, discovers topic subdirectories,
 * and catalogs all regular files into memory. Sorts files alphabetically
 * for deterministic multi-part indexing.
 * Returns 0 on success, or -1 on error.
 */
int topic_registry_init(TopicRegistry *reg, const char *root_dir);

/*
 * topic_registry_find:
 * Looks up a topic by name using case-insensitive comparison (strcasecmp).
 * Protected by pthread_rwlock_rdlock for thread safety.
 * Returns pointer to TopicRecord if found, or NULL if not found.
 */
const TopicRecord *topic_registry_find(TopicRegistry *reg, const char *topic_name);

/*
 * topic_registry_destroy:
 * Frees all allocated file arrays and destroys the synchronization lock.
 */
void topic_registry_destroy(TopicRegistry *reg);

/*
 * topic_registry_print_summary:
 * Utility function to print cataloged topics and file counts on server startup.
 */
void topic_registry_print_summary(const TopicRegistry *reg);

#endif /* TOPIC_MANAGER_H */

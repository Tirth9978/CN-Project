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

typedef struct {
    char filename[MAX_FILENAME_LEN];
    char file_path[PATH_MAX];
    uint64_t file_size;
    uint32_t file_index;
} FileRecord;

typedef struct {
    char topic_name[MAX_TOPIC_NAME_LEN];
    char directory_path[PATH_MAX];
    uint32_t file_count;
    uint32_t capacity;
    uint64_t total_topic_bytes;
    FileRecord *files;
} TopicRecord;

typedef struct {
    TopicRecord topics[MAX_TOPICS];
    uint32_t topic_count;
    char root_dir[PATH_MAX];
    pthread_rwlock_t rwlock;
} TopicRegistry;

int topic_registry_init(TopicRegistry *reg, const char *root_dir);
const TopicRecord *topic_registry_find(TopicRegistry *reg, const char *topic_name);
void topic_registry_destroy(TopicRegistry *reg);
void topic_registry_print_summary(const TopicRegistry *reg);

#endif /* TOPIC_MANAGER_H */

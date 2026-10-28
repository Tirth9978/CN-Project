#include "topic_manager.h"

static void safe_copy(char *dest, size_t dest_sz, const char *src) {
    if (!dest || dest_sz == 0) return;
    if (!src) {
        dest[0] = '\0';
        return;
    }
    size_t len = strlen(src);
    if (len >= dest_sz) {
        len = dest_sz - 1;
    }
    memcpy(dest, src, len);
    dest[len] = '\0';
}

static int compare_file_records(const void *a, const void *b) {
    const FileRecord *fa = (const FileRecord *)a;
    const FileRecord *fb = (const FileRecord *)b;
    return strcmp(fa->filename, fb->filename);
}

int topic_registry_init(TopicRegistry *reg, const char *root_dir) {
    if (!reg || !root_dir) {
        return -1;
    }

    memset(reg, 0, sizeof(TopicRegistry));
    if (pthread_rwlock_init(&reg->rwlock, NULL) != 0) {
        perror("pthread_rwlock_init");
        return -1;
    }

    safe_copy(reg->root_dir, sizeof(reg->root_dir), root_dir);

    DIR *dir = opendir(root_dir);
    if (!dir) {
        perror("opendir root_dir");
        pthread_rwlock_destroy(&reg->rwlock);
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') {
            continue;
        }

        char subpath[PATH_MAX];
        size_t root_len = strlen(root_dir);
        size_t name_len = strlen(entry->d_name);
        if (root_len + 1 + name_len >= sizeof(subpath)) {
            continue;
        }
        memcpy(subpath, root_dir, root_len);
        subpath[root_len] = '/';
        memcpy(subpath + root_len + 1, entry->d_name, name_len);
        subpath[root_len + 1 + name_len] = '\0';

        struct stat st;
        if (stat(subpath, &st) != 0) {
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            continue;
        }

        if (reg->topic_count >= MAX_TOPICS) {
            fprintf(stderr, "[WARN] Maximum topic count reached (%d), skipping %s\n", MAX_TOPICS, entry->d_name);
            break;
        }

        TopicRecord *t = &reg->topics[reg->topic_count];
        safe_copy(t->topic_name, sizeof(t->topic_name), entry->d_name);
        safe_copy(t->directory_path, sizeof(t->directory_path), subpath);

        t->capacity = INITIAL_FILES_CAP;
        t->files = malloc(sizeof(FileRecord) * t->capacity);
        if (!t->files) {
            perror("malloc files");
            continue;
        }
        t->file_count = 0;
        t->total_topic_bytes = 0;

        DIR *subdir = opendir(subpath);
        if (!subdir) {
            perror("opendir subpath");
            free(t->files);
            t->files = NULL;
            continue;
        }

        struct dirent *file_entry;
        while ((file_entry = readdir(subdir)) != NULL) {
            if (file_entry->d_name[0] == '.') {
                continue;
            }

            char filepath[PATH_MAX];
            size_t sub_len = strlen(subpath);
            size_t fname_len = strlen(file_entry->d_name);
            if (sub_len + 1 + fname_len >= sizeof(filepath)) {
                continue;
            }
            memcpy(filepath, subpath, sub_len);
            filepath[sub_len] = '/';
            memcpy(filepath + sub_len + 1, file_entry->d_name, fname_len);
            filepath[sub_len + 1 + fname_len] = '\0';

            struct stat fst;
            if (stat(filepath, &fst) != 0) {
                continue;
            }

            if (!S_ISREG(fst.st_mode)) {
                continue;
            }

            if (t->file_count >= t->capacity) {
                if (t->capacity * 2 <= MAX_FILES_PER_TOPIC) {
                    uint32_t new_cap = t->capacity * 2;
                    FileRecord *new_files = realloc(t->files, sizeof(FileRecord) * new_cap);
                    if (!new_files) {
                        perror("realloc files");
                        break;
                    }
                    t->files = new_files;
                    t->capacity = new_cap;
                } else {
                    fprintf(stderr, "[WARN] Topic %s exceeded max files (%d)\n", t->topic_name, MAX_FILES_PER_TOPIC);
                    break;
                }
            }

            FileRecord *f = &t->files[t->file_count];
            safe_copy(f->filename, sizeof(f->filename), file_entry->d_name);
            safe_copy(f->file_path, sizeof(f->file_path), filepath);
            f->file_size = (uint64_t)fst.st_size;
            f->file_index = t->file_count;

            t->total_topic_bytes += f->file_size;
            t->file_count++;
        }
        closedir(subdir);

        if (t->file_count > 1) {
            qsort(t->files, t->file_count, sizeof(FileRecord), compare_file_records);
            for (uint32_t i = 0; i < t->file_count; i++) {
                t->files[i].file_index = i;
            }
        }

        reg->topic_count++;
    }
    closedir(dir);

    return 0;
}

const TopicRecord *topic_registry_find(TopicRegistry *reg, const char *topic_name) {
    if (!reg || !topic_name) {
        return NULL;
    }

    pthread_rwlock_rdlock(&reg->rwlock);

    const TopicRecord *match = NULL;
    for (uint32_t i = 0; i < reg->topic_count; i++) {
        if (strcasecmp(reg->topics[i].topic_name, topic_name) == 0) {
            match = &reg->topics[i];
            break;
        }
    }

    pthread_rwlock_unlock(&reg->rwlock);
    return match;
}

void topic_registry_destroy(TopicRegistry *reg) {
    if (!reg) return;

    pthread_rwlock_wrlock(&reg->rwlock);
    for (uint32_t i = 0; i < reg->topic_count; i++) {
        if (reg->topics[i].files) {
            free(reg->topics[i].files);
            reg->topics[i].files = NULL;
        }
    }
    reg->topic_count = 0;
    pthread_rwlock_unlock(&reg->rwlock);

    pthread_rwlock_destroy(&reg->rwlock);
}

void topic_registry_print_summary(const TopicRegistry *reg) {
    if (!reg) return;

    printf("====================================================\n");
    printf(" Topic Registry Catalog Summary (Root: %s)\n", reg->root_dir);
    printf(" Total Topics Discovered: %u\n", reg->topic_count);
    printf("----------------------------------------------------\n");
    for (uint32_t i = 0; i < reg->topic_count; i++) {
        const TopicRecord *t = &reg->topics[i];
        double mb = (double)t->total_topic_bytes / (1024.0 * 1024.0);
        printf(" [%2u] Topic: %-15s | Files: %5u | Size: %8.2f MB\n",
               i + 1, t->topic_name, t->file_count, mb);
    }
    printf("====================================================\n");
}

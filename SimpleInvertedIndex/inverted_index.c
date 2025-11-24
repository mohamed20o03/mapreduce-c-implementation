// Simple Inverted Index - validates MapReduce output
// Usage: ./inverted_index file1.txt file2.txt ...

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <sys/resource.h>

#define HASH_SIZE 131071

typedef struct DocNode {
    char *name;
    struct DocNode *next;
} DocNode;

typedef struct Entry {
    char word[256];
    DocNode *docs;
    int doc_count;
    struct Entry *next;
} Entry;

static Entry *hash_table[HASH_SIZE];
static int num_words = 0;

static unsigned int hash_word(const char *str) {
    unsigned int h = 5381;
    while (*str) {
        h = ((h << 5) + h) + (unsigned char)(*str++);
    }
    return h % HASH_SIZE;
}

static void add_posting(const char *word, const char *filename) {
    unsigned int bucket = hash_word(word);
    Entry *entry = hash_table[bucket];

    while (entry) {
        if (strcmp(entry->word, word) == 0) {
            break;
        }
        entry = entry->next;
    }

    if (!entry) {
        entry = malloc(sizeof(Entry));
        if (!entry) exit(1);
        strcpy(entry->word, word);
        entry->docs = NULL;
        entry->doc_count = 0;
        entry->next = hash_table[bucket];
        hash_table[bucket] = entry;
        num_words++;
    }

    DocNode *doc = entry->docs;
    while (doc) {
        if (strcmp(doc->name, filename) == 0) {
            return; /* Already recorded for this document */
        }
        doc = doc->next;
    }

    DocNode *node = malloc(sizeof(DocNode));
    if (!node) exit(1);
    node->name = strdup(filename);
    if (!node->name) exit(1);
    node->next = entry->docs;
    entry->docs = node;
    entry->doc_count++;
}

static void flush_word(const char *word, const char *filename) {
    char lower[256];
    size_t len = strlen(word);
    if (len >= sizeof(lower)) len = sizeof(lower) - 1;
    for (size_t i = 0; i < len; i++) {
        lower[i] = (char)tolower((unsigned char)word[i]);
    }
    lower[len] = '\0';
    if (len > 0) {
        add_posting(lower, filename);
    }
}

static int compare_entries(const void *a, const void *b) {
    const Entry *ea = *(const Entry **)a;
    const Entry *eb = *(const Entry **)b;
    return strcmp(ea->word, eb->word);
}

static int compare_docs(const void *a, const void *b) {
    const char *const *sa = (const char *const *)a;
    const char *const *sb = (const char *const *)b;
    return strcmp(*sa, *sb);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s file1.txt file2.txt ...\n", argv[0]);
        return 1;
    }

    struct timeval start, end;
    struct rusage usage_start, usage_end;
    gettimeofday(&start, NULL);
    getrusage(RUSAGE_SELF, &usage_start);

    for (int i = 1; i < argc; i++) {
        FILE *fp = fopen(argv[i], "r");
        if (!fp) {
            fprintf(stderr, "Warning: failed to open %s\n", argv[i]);
            continue;
        }

        char word[256];
        int wlen = 0;
        int ch;
        while ((ch = fgetc(fp)) != EOF) {
            if (isalnum(ch)) {
                if (wlen < (int)sizeof(word) - 1) {
                    word[wlen++] = (char)tolower(ch);
                }
            } else if (wlen > 0) {
                word[wlen] = '\0';
                flush_word(word, argv[i]);
                wlen = 0;
            }
        }
        if (wlen > 0) {
            word[wlen] = '\0';
            flush_word(word, argv[i]);
        }
        fclose(fp);
    }

    Entry **entries = malloc(num_words * sizeof(Entry *));
    if (!entries) exit(1);
    int idx = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        Entry *entry = hash_table[i];
        while (entry) {
            entries[idx++] = entry;
            entry = entry->next;
        }
    }
    qsort(entries, num_words, sizeof(Entry *), compare_entries);

    FILE *out = fopen("simple_output.txt", "w");
    if (!out) exit(1);

    for (int i = 0; i < num_words; i++) {
        Entry *entry = entries[i];
        if (entry->doc_count == 0) {
            continue;
        }
        char **docs = malloc(entry->doc_count * sizeof(char *));
        if (!docs) exit(1);
        int d = 0;
        for (DocNode *node = entry->docs; node; node = node->next) {
            docs[d++] = node->name;
        }
        qsort(docs, entry->doc_count, sizeof(char *), compare_docs);

        fprintf(out, "%s -> [", entry->word);
        for (int j = 0; j < entry->doc_count; j++) {
            fprintf(out, "%s%s", docs[j], (j + 1 < entry->doc_count) ? ", " : "");
        }
        fprintf(out, "]\n");
        free(docs);
    }
    fclose(out);

    gettimeofday(&end, NULL);
    getrusage(RUSAGE_SELF, &usage_end);

    double wall_time = (end.tv_sec - start.tv_sec) +
                       (end.tv_usec - start.tv_usec) / 1e6;
    double user_cpu = (usage_end.ru_utime.tv_sec - usage_start.ru_utime.tv_sec) +
                      (usage_end.ru_utime.tv_usec - usage_start.ru_utime.tv_usec) / 1e6;
    double sys_cpu = (usage_end.ru_stime.tv_sec - usage_start.ru_stime.tv_sec) +
                     (usage_end.ru_stime.tv_usec - usage_start.ru_stime.tv_usec) / 1e6;

    printf("\n===== Simple Inverted Index =====\n");
    printf("Unique words: %d\n", num_words);
    printf("Wall time   : %.3f sec\n", wall_time);
    printf("User CPU    : %.3f sec\n", user_cpu);
    printf("System CPU  : %.3f sec\n", sys_cpu);
    printf("Total CPU   : %.3f sec\n", user_cpu + sys_cpu);
    printf("Output: simple_output.txt\n");
    printf("=================================\n");

    free(entries);
    return 0;
}

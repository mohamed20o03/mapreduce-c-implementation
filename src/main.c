/**
 * main.c - MapReduce Application Entry Point
 * 
 * Implements a word-count MapReduce application with configurable
 * readers, mappers, and reducers. Demonstrates the MapReduce API.
 * 
 * Features:
 * - Command-line configuration (-i/-m/-r flags)
 * - Performance timing and resource usage tracking
 * - Parallel I/O with chunk-level reader threads
 * - Thread-local buffering for efficient map operations
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "mapreduce.h"
#include "job.h"

/* Output directory used by reducers when writing files */
const char *output_dir = "output";

/* ----- User-provided Map and Reduce functions (inverted index) ----- */
/* Map: parse chunk into words and emit (word, filename) pairs */
void Map(char *data, int size) {
    if (!data || size <= 0) return;

    /* Make a nul-terminated copy so we can walk safely */
    char *buf = malloc(size + 1);
    if (!buf) return;
    memcpy(buf, data, size);
    buf[size] = '\0';

    char word[256];
    int wlen = 0;
    for (int i = 0; buf[i] != '\0'; i++) {
        unsigned char c = (unsigned char)buf[i];
        if (isalnum(c)) {
            if (wlen < (int)sizeof(word) - 1) {
                word[wlen++] = tolower(c);
            }
        } else {
            if (wlen > 0) {
                word[wlen] = '\0';
                MR_Emit(word, (char *)MR_CurrentFile());
                wlen = 0;
            }
        }
    }
    if (wlen > 0) {
        word[wlen] = '\0';
        MR_Emit(word, (char *)MR_CurrentFile());
    }

    free(buf);
}

/* Combiner: drop duplicate filenames per key inside mapper buffer */
static void DedupCombiner(char *key, value_node_t **head_ref) {
    (void)key;
    if (!head_ref || !*head_ref) return;

    for (value_node_t *outer = *head_ref; outer; outer = outer->next_value_node) {
        value_node_t *prev = outer;
        value_node_t *curr = outer->next_value_node;
        while (curr) {
            if (strcmp(curr->value_name, outer->value_name) == 0) {
                prev->next_value_node = curr->next_value_node;
                free(curr->value_name);
                free(curr);
                curr = prev->next_value_node;
            } else {
                prev = curr;
                curr = curr->next_value_node;
            }
        }
    }
}

static int compare_doc_names(const void *a, const void *b) {
    const char *const *sa = (const char *const *)a;
    const char *const *sb = (const char *const *)b;
    return strcmp(*sa, *sb);
}

/* Reduce: aggregate document names per key and append postings list */
void Reduce(char *key, Getter get_next, int partition_number) {
    /* Use thread-local FILE* so each reducer opens its file once */
    static __thread FILE *out = NULL;
    if (!out) {
        char path[256];
        snprintf(path, sizeof(path), "%s/part-%05d.txt", output_dir, partition_number);
        out = fopen(path, "a");
        if (!out) return; /* best-effort: skip on failure */
    }

    size_t capacity = 64;
    size_t count = 0;
    char **docs = malloc(capacity * sizeof(char *));
    if (!docs) return;

    char *val;
    while ((val = get_next(key, partition_number)) != NULL) {
        if (count == capacity) {
            capacity *= 2;
            char **tmp = realloc(docs, capacity * sizeof(char *));
            if (!tmp) {
                free(docs);
                return;
            }
            docs = tmp;
        }
        docs[count++] = val;
    }

    if (count == 0) {
        free(docs);
        return;
    }

    qsort(docs, count, sizeof(char *), compare_doc_names);

    size_t unique = 0;
    for (size_t i = 0; i < count; i++) {
        if (unique == 0 || strcmp(docs[i], docs[unique - 1]) != 0) {
            docs[unique++] = docs[i];
        }
    }

    fprintf(out, "%s -> [", key);
    for (size_t i = 0; i < unique; i++) {
        fprintf(out, "%s%s", docs[i], (i + 1 < unique) ? ", " : "");
    }
    fprintf(out, "]\n");
    fflush(out);
    free(docs);
}

int main(int argc, char *argv[]) {
    MR_Job job = MR_DefaultJob();
    job.map = Map;
    job.reduce = Reduce;
    job.combiner = DedupCombiner; /* optional; set NULL to disable */
    /* Users can override -i/-m/-r via CLI; defaults are in MR_DefaultJob */
    return MR_Run(&job, argc, argv);
}
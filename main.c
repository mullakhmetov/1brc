#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

#define MAX_LINE_LENGTH 1 << 10
#define MAP_SIZE (1 << 12)
#define MAX_RESULTS 450

#ifndef NTHREADS
#define NTHREADS 8
#endif


typedef unsigned long long ull;

void parse_city(ull start, ull end, char *data, char *city)
{
    int i = 0;
    while (start + i < end) {
        city[i] = data[start + i];
        i++;
    }
    city[i] = '\0';
}

int hash(char *data, ull offset, int len)
{
    unsigned int h = 0;
    for (int i = 0; i < len; i++) {
        h = (h * 31) + data[offset + i];
    }

    return h & (MAP_SIZE - 1);
}

ull get_fsize(int f_ptr)
{
    struct stat st;
    int err = fstat(f_ptr, &st);
    if (err)
        return -1;

    return st.st_size;
}

ull get_next_byte_pos(ull offset, char *data, ull end, int b)
{
    for (ull i = offset; i < end; i++) {
        if (data[i] == b)
            return i;
    }

    return -1;
}

typedef struct {
    char *data;
    ull start_p;
    ull end_p;
} Chunk;

typedef struct
{
    char city[100];
    ull city_offset;
    int city_len;
    int count;
    int sum, min, max;
} Result;

typedef struct {
    Result *results;
    int n_results;
    ull lines;
} ChunkResults;

void print_results(Result results[MAX_RESULTS], int n_results)
{
    printf("{");
    for (int i = 0; i < n_results; i++) {
        printf("%s=%.1f/%.1f/%.1f%s",
               results[i].city,
               results[i].min / 10.0,
               (double)(results[i].sum) / results[i].count / 10.0,
               results[i].max / 10.0,
               i + 1 < n_results ? ", " : "");
    }
    printf("}\n");
}

int results_cmp(const void *a, const void *b) {
    return strcmp(((Result*)a)->city, ((Result*)b)->city);
}

void *process_chunk(void *args)
{
    Chunk *chunk = args;
    ChunkResults *chunk_results = malloc(sizeof *chunk_results);
    Result *results = malloc(sizeof(Result) * MAX_RESULTS);
    int n_results = 0;
    ull lines = 0;

    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));

    ull p = chunk->start_p;
    ull new_p = p;
    int temp;
    int sign;
    int city_len;
    unsigned int h;

    while (p < chunk->end_p) {
        // city len & hash
        city_len = 0;
        h = 0;
        while (chunk->data[new_p] != ';') {
            h = h * 31 + chunk->data[new_p];
            city_len++;
            new_p++;
        }
        h = h & (MAP_SIZE - 1);

        // temp
        new_p++;
        temp = 0;
        if (chunk->data[new_p] == '-') {
            new_p++;
            sign = -1;
        } else {
            sign = 1;
        }

        while(chunk->data[new_p] != '\n') {
            if (chunk->data[new_p] != '.') {
                temp *= 10;
                temp += chunk->data[new_p] - 48; // ascii byte-code to int
            }
            new_p++;
        }
        temp *= sign;

        // saving result
        while (map[h] != -1 &&
               results[map[h]].city_len != city_len &&
               memcmp(chunk->data + results[map[h]].city_offset,
                      chunk->data + p,
                      results[map[h]].city_len) != 0) {
            h = (h + 1) & (MAP_SIZE - 1);
        }

        if (map[h] < 0) {
            map[h] = n_results;
            results[n_results].city_offset = p;
            results[n_results].city_len = city_len;
            results[n_results].count = 1;
            results[n_results].sum = temp;
            results[n_results].min = temp;
            results[n_results].max = temp;

            n_results++;
        } else {
            results[map[h]].count++;
            results[map[h]].sum += temp;
            if (temp < results[map[h]].min)
                results[map[h]].min = temp;
            if (temp > results[map[h]].max)
                results[map[h]].max = temp;
        }

        p = new_p + 1;
        lines++;
	}

    chunk_results->results = results;
    chunk_results->n_results = n_results;
    chunk_results->lines = lines;

    printf("debug %llu lines\n", lines);

    return (void *)chunk_results;
}

int main(void)
{
    int f_ptr = open("measurements.txt", O_RDONLY);
    if (f_ptr == 0) {
		fprintf(stderr, "file open error! [%s]\n", strerror(errno));
        return -1;
    }
    ull f_sz = get_fsize(f_ptr);
    if (f_sz < 0) {
		fprintf(stderr, "fstat error! [%s]\n", strerror(errno));
        return -1;
    }

    char *data = mmap(NULL, f_sz, PROT_READ, MAP_SHARED, (long)f_ptr, 0);
    if (data == MAP_FAILED) {
        perror("error mmapping file");
        return -1;
    }

    // split data into N chunks and start N threads
    pthread_t workers[NTHREADS];
    Chunk *chunks = malloc(sizeof(Chunk) * NTHREADS);

    ull chunk_start = 0;
    ull est_chunk_end;
    ull chunk_end;
    ull base_chunk_size = f_sz / NTHREADS;
    for (int i = 0; i < NTHREADS; i++) {
        est_chunk_end = chunk_start + base_chunk_size - 1;
        if (est_chunk_end > f_sz) {
            chunk_end = f_sz;
        } else {
            chunk_end = get_next_byte_pos(est_chunk_end, data, f_sz, '\n');
            if (chunk_end == (ull)-1) {
                perror("unexpected EOF");
                return -1;
            }
            chunk_end++;
        }

        chunks[i].data = data;
        chunks[i].start_p = chunk_start;
        chunks[i].end_p = chunk_end;

        if(pthread_create(&workers[i], NULL, process_chunk, &chunks[i])) {
            free(chunks);
            perror("error thread creating");
            return -1;
        }

        printf("debug: thread %d started for %llu chunk size\n", i, chunk_end - chunk_start);

        chunk_start = chunk_end;
    }


    ChunkResults *chunk_results[NTHREADS];
    for (int i = 0; i < NTHREADS; i++) {
        pthread_join(workers[i], (void *)&chunk_results[i]);
        printf("debug thread join %d\n", i);
    }

    // join chunk results
    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));
    Result results[MAX_RESULTS];
    int n_results = 0;
    ull lines = 0;

    for (int i = 0; i < NTHREADS; i++) {
        for (int ri = 0; ri < chunk_results[i]->n_results; ri++) {
            Result ch_result = chunk_results[i]->results[ri];
            unsigned int h = hash(data, ch_result.city_offset, ch_result.city_len);
            while (map[h] != -1 &&
                   results[map[h]].city_len != ch_result.city_len &&
                   memcmp(data + results[map[h]].city_offset, data + ch_result.city_offset, ch_result.city_len) != 0) {
                h = (h + 1) & (MAP_SIZE - 1);
            }

            if (map[h] < 0) {
                map[h] = n_results;
                parse_city(ch_result.city_offset, ch_result.city_offset + ch_result.city_len, data, results[n_results].city);
                results[n_results].city_offset = ch_result.city_offset;
                results[n_results].city_len = ch_result.city_len;
                results[n_results].count = ch_result.count;
                results[n_results].min = ch_result.min;
                results[n_results].max = ch_result.max;
                results[n_results].sum = ch_result.sum;

                n_results++;
            } else {
                results[map[h]].count += ch_result.count;
                results[map[h]].sum += ch_result.sum;
                if (ch_result.min < results[map[h]].min)
                    results[map[h]].min = ch_result.min;
                if (ch_result.max > results[map[h]].max)
                    results[map[h]].max = ch_result.max;
            }
        }
        lines += chunk_results[i]->lines;

        free(chunk_results[i]->results);
        free(chunk_results[i]);
    }

    free(chunks);

    int err = munmap((void *)data, f_sz);
    if (err)
		fprintf(stderr, "fstat error! [%s]\n", strerror(errno));

    close(f_ptr);

    qsort(results, (size_t)n_results, sizeof(*results), results_cmp);
    print_results(results, n_results);

    printf("finished %llu lines\n", lines);

    return 0;
}


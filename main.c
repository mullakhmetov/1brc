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

int parse_temp(ull start, ull end, char *data)
{
    int result = 0;
    int i, sign;

    if (data[start] == '-') {
        i = 1;
        sign = -1;
    } else {
        i = 0;
        sign = 1;
    }
    
    while(start + i < end) {
        if (data[start + i] != '.') {
            result *= 10;
            result += data[start + i] - 48;
        }
        i++;
    }
    
    return result * sign;
}

int hash(char *data, int n)
{
    unsigned int hash = 0;
    for (int i = 0; i < n; i++) {
        hash = (hash * 31) + data[i];
    }

    return hash;
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
    int city_len;
    int count;
    int sum, min, max;
} Result;

typedef struct {
    Result *results;
    int n_results;
    int lines;
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
    ull start_l = chunk->start_p;
    ull end_l = chunk->start_p;

    ChunkResults *chunk_results = malloc(sizeof *chunk_results);
    Result *results = malloc(sizeof(Result) * MAX_RESULTS);
    int n_results = 0;

    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));

    ull semicolon_p = 0;
    int temp;
    char city[100];

    while (end_l >= 0 && start_l < chunk->end_p) {
        end_l = get_next_byte_pos(start_l, chunk->data, chunk->end_p, '\n'); 
        semicolon_p = get_next_byte_pos(start_l, chunk->data, chunk->end_p, ';');

        temp = parse_temp(semicolon_p + 1, end_l, chunk->data);
        parse_city(start_l, semicolon_p, chunk->data, city);

        unsigned int h = hash(city, semicolon_p - start_l) & (MAP_SIZE - 1);
        while (map[h] != -1 && strcmp(results[map[h]].city, city) != 0) {
            h = (h + 1) & (MAP_SIZE - 1);
        }

        if (map[h] < 0) {
            map[h] = n_results;
            strcpy(results[n_results].city, city);
            results[n_results].city_len = semicolon_p - start_l;
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

        start_l = end_l + 1;
        chunk_results->lines++;
	}

    chunk_results->results = results;
    chunk_results->n_results = n_results;

    return (void *)chunk_results;
}

int main(void)
{
    int f_ptr = open("measurements_100M.txt", O_RDONLY);
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
        est_chunk_end = chunk_start + base_chunk_size;
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
            unsigned int h = hash(ch_result.city, ch_result.city_len) & (MAP_SIZE - 1);
            while (map[h] != -1 && strcmp(results[map[h]].city, ch_result.city) != 0) {
                h = (h + 1) & (MAP_SIZE - 1);
            }

            if (map[h] < 0) {
                map[h] = n_results;
                strcpy(results[n_results].city, ch_result.city);
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


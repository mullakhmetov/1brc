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
#define DELIMITER ";"
#define MAP_SIZE (1 << 12)
#define NEWLINE_B 0x0A
#define SEMICOLON_B 0x3B
#define MINUS_B 0x2D
#define PERIOD_B 0x2E
#define MAX_RESULTS 450

#ifndef NTHREADS
#define NTHREADS 8
#endif


typedef unsigned long long ull;

typedef struct
{
    char city[100];
    int city_len;
    int count;
    int sum, min, max;
} Result;

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
    ull lines;
    Result *results;
    int n_results;
} Chunk;


void *process_chunk(void *args)
{
    Chunk *chunk = args;
    ull start_l = chunk->start_p;
    ull end_l = chunk->start_p;

    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));

    ull semicolon_p = 0;
    int temp;
    char city[100];

    while (end_l >= 0 && start_l < chunk->end_p) {
        end_l = get_next_byte_pos(start_l, chunk->data, chunk->end_p, NEWLINE_B); 
        semicolon_p = get_next_byte_pos(start_l, chunk->data, chunk->end_p, SEMICOLON_B);

        temp = parse_temp(semicolon_p + 1, end_l, chunk->data);
        parse_city(start_l, semicolon_p, chunk->data, city);

        unsigned int h = hash(city, semicolon_p - start_l) & (MAP_SIZE - 1);
        while (map[h] != -1 && strcmp(chunk->results[map[h]].city, city) != 0) {
            h = (h + 1) & (MAP_SIZE - 1);
        }

        if (map[h] < 0) {
            map[h] = chunk->n_results;
            strcpy(chunk->results[chunk->n_results].city, city);
            chunk->results[chunk->n_results].city_len = semicolon_p - start_l;
            chunk->results[chunk->n_results].count = 1;
            chunk->results[chunk->n_results].sum = temp;
            chunk->results[chunk->n_results].min = temp;
            chunk->results[chunk->n_results].max = temp;

            chunk->n_results++;
        } else {
            strcpy(chunk->results[map[h]].city, city);
            chunk->results[map[h]].count++;
            chunk->results[map[h]].sum += temp;
            if (temp < chunk->results[map[h]].min)
                chunk->results[map[h]].min = temp;
            if (temp > chunk->results[map[h]].max)
                chunk->results[map[h]].max = temp;
        }

        start_l = end_l + 1;
        chunk->lines++;
	}
    return 0;
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
    Result *chunks_results = malloc(sizeof(Result) * NTHREADS * MAX_RESULTS);

    ull chunk_start = 0;
    ull est_chunk_end;
    ull chunk_end;
    ull base_chunk_size = f_sz / NTHREADS;
    for (int i = 0; i < NTHREADS; i++) {
        est_chunk_end = chunk_start + base_chunk_size;
        if (est_chunk_end >= f_sz) {
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
        chunks[i].lines = 0;
        chunks[i].results = &chunks_results[i];
        chunks[i].n_results = 0;

        if(pthread_create(&workers[i], NULL, process_chunk, &chunks[i])) {
            free(chunks_results);
            free(chunks);
            perror("error thread creating");
            return -1;
        }

        chunk_start = chunk_end + 1;
    }

    for (int i = 0; i < NTHREADS; i++) {
        pthread_join(workers[i], NULL);
        printf("debug thread join %d\n", i);
    }

    // join chunk results
    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));
    Result results[MAX_RESULTS];
    int n_results = 0;
    ull lines = 0;

    for (int i = 0; i < NTHREADS; i++) {
        for (int ri = 0; ri < chunks[i].n_results; ri++) {
            Result ch_result = chunks[i].results[ri];
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
        lines += chunks[i].lines;
    }

    free(chunks_results);
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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define MAX_LINE_LENGTH 1 << 10
#define LIMIT (-1)
#define DELIMITER ";"
#define MAP_SIZE (1 << 12)
#define NEWLINE_B 0x0A
#define SEMICOLON_B 0x3B
#define MINUS_B 0x2D
#define PERIOD_B 0x2E


typedef unsigned long long ull;

struct result
{
    char city[100];
    int count;
    int min, max, sum;
};

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

void print_results(struct result results[450], int n_results)
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
    return strcmp(((struct result*)a)->city, ((struct result*)b)->city);
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

ull get_next_byte_pos(ull offset, char *data, ull size, int b)
{
    for (ull i = offset; i < size; i++) {
        if (data[i] == b)
            return i;
    }

    return -1;
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

    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));

    struct result results[450];
    int n_results = 0;

    char *data = mmap(NULL, f_sz, PROT_READ, MAP_SHARED, (long)f_ptr, 0);
    if (data == MAP_FAILED) {
        perror("error mmapping file");
        return -1;
    }

    ull lines = 0;
    ull start_l = 0;
    ull semicolon_p = 0;
    ull end_l = 0;
    int temp;
    char city[100];
    while (end_l >= 0 && start_l < f_sz && (LIMIT < 0 || lines < LIMIT)) {
        end_l = get_next_byte_pos(start_l, data, f_sz, NEWLINE_B); 
        semicolon_p = get_next_byte_pos(start_l, data, f_sz, SEMICOLON_B);

        temp = parse_temp(semicolon_p + 1, end_l, data);
        parse_city(start_l, semicolon_p, data, city);

        unsigned int h = hash(city, semicolon_p - start_l) & (MAP_SIZE - 1);
        while (map[h] != -1 && strcmp(results[map[h]].city, city) != 0) {
            h = (h + 1) & (MAP_SIZE - 1);
        }

        if (map[h] < 0) {
            map[h] = n_results;
            strcpy(results[n_results].city, city);
            results[n_results].count = 1;
            results[n_results].min = temp;
            results[n_results].max = temp;
            results[n_results].sum = temp;

            n_results++;
        } else {
            strcpy(results[map[h]].city, city);
            results[map[h]].count++;
            if (temp < results[map[h]].min)
                results[map[h]].min = temp;
            if (temp > results[map[h]].max)
                results[map[h]].max = temp;
            results[map[h]].sum += temp;
        }

        lines++;

        start_l = end_l + 1;
	}

    int err = munmap((void *)data, f_sz);
    if (err)
		fprintf(stderr, "fstat error! [%s]\n", strerror(errno));

    close(f_ptr);

    qsort(results, (size_t)n_results, sizeof(*results), results_cmp);
    print_results(results, n_results);

    printf("finished %llu lines\n", lines);

    return 0;
}


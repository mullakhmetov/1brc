#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 1 << 10
#define LIMIT (10)
#define DELIMITER ";"
#define MAP_SIZE (1 << 12)

struct result
{
    char city[100];
    int count;
    double min, max, sum;
};

int parse_city(char *str_in, char *city)
{
    int i = 0;
    while (str_in[i] != ';') {
        city[i] = str_in[i];
        i++;
    }
    city[i] = '\0';

    return i;
}

double parse_temp(int pos, char *str_in)
{
    char buf[6];
    int i = 0;
    while(str_in[i + pos] != '\n') {
        buf[i] = str_in[i + pos];
        i++;
    }
    buf[i] = '\0';

    return atof(buf);
}

void print_results(struct result results[450], int n_results)
{
    printf("{");
    for (int i = 0; i < n_results; i++) {
        printf("%s=%.1f/%.1f/%.1f%s",
               results[i].city,
               results[i].min,
               results[i].sum / results[i].count,
               results[i].max,
               i + 1 < n_results ? ", " : "");
    }
    printf("}\n");
}

int results_cmp(const void *a, const void *b) {
    return strcmp(((struct result*)a)->city, ((struct result*)b)->city);
}

static unsigned int hash(char *data, int n)
{
    unsigned int hash = 0;
    for (int i = 0; i < n; i++) {
        hash = (hash * 31) + data[i];
    }

    return hash;
}

int main(void)
{
    FILE * f_ptr;
    f_ptr = fopen("measurements.txt", "r");
    if (f_ptr == NULL) {
        printf("Error opening file!\n");
        return 1;
    }
   
    int map[MAP_SIZE];
    memset(map, -1, sizeof(map));

    struct result results[450];
    int n_results = 0;
    long i = 0;

    char buf[MAX_LINE_LENGTH];
    char city[100];
    while (fgets(buf, MAX_LINE_LENGTH, f_ptr) != NULL && (LIMIT < 0 || i < LIMIT)) {
        int delimiter_pos = parse_city(buf, city);
        double temp = parse_temp(delimiter_pos + 1, buf);

        unsigned int h = hash(city, delimiter_pos) & (MAP_SIZE - 1);
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
        
        i++;
    }
    
    fclose(f_ptr);

    qsort(results, (size_t)n_results, sizeof(*results), results_cmp);
    print_results(results, n_results);

    printf("finished %ld lines\n", i);

    return 0;
}


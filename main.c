#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 1 << 10
#define LIMIT -1
#define DELIMITER ";"

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

int get_city_idx(char *city, struct result *results)
{
    for (int i = 0; i < 450; i++) {
        if (strcmp(city, results[i].city) == 0) {
            return i;
        }
    }

    return -1;
}

void print_results(struct result results[450], int n_results)
{
    for (int i = 0; i < n_results; i++) {
        printf("%s=%.1f/%.1f/%.1f\n",
               results[i].city,
               results[i].min,
               results[i].sum / results[i].count,
               results[i].max);
    }
}

int results_cmp(const void *a, const void *b) {
    return strcmp(((struct result*)a)->city, ((struct result*)b)->city);
}

int main(void)
{
    FILE * f_ptr;
    f_ptr = fopen("measurements.txt", "r");
    if (f_ptr == NULL) {
        printf("Error opening file!\n");
        return 1;
    }
    
    struct result results[450];
    int n_results = 0;
    long i = 0;

    char buf[MAX_LINE_LENGTH];
    while (fgets(buf, MAX_LINE_LENGTH, f_ptr) != NULL && (LIMIT < 0 || i < LIMIT)) {
        char city[100];
        int delimiter_pos = parse_city(buf, city);
        double temp = parse_temp(delimiter_pos + 1, buf);

        int c_idx = get_city_idx(city, results);
        if (c_idx < 0) {
            strcpy(results[n_results].city, city);
            results[n_results].count = 1;
            results[n_results].min = temp;
            results[n_results].max = temp;
            results[n_results].sum = temp;

            n_results++;
        } else {
            strcpy(results[c_idx].city, city);
            results[c_idx].count++;
            if (temp < results[c_idx].min)
                results[c_idx].min = temp;
            if (temp > results[c_idx].max)
                results[c_idx].max = temp;
            results[c_idx].sum += temp;
        }
        
        i++;
    }
    
    fclose(f_ptr);

    qsort(results, (size_t)n_results, sizeof(*results), results_cmp);
    print_results(results, n_results);

    printf("finished %ld lines\n", i);

    return 0;
}


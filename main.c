#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 1 << 10
#define LIMIT 10 
#define DELIMITER ";"

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

float parse_temp(int pos, char *str_in, char *temp)
{
    for (int i = 0; i < 5; i++) {
        temp[i] = str_in[pos + i];
    }
    temp[5] = '\0';
    int i = pos;
    while(str_in[i] != 'str_in

    return atof(temp);
}

int main(void)
{
    FILE * f_ptr;
    f_ptr = fopen("measurements.txt", "r");
    if (f_ptr == NULL) {
        printf("Error opening file!\n");
        return 1;
    }
   
    long i = 0;
    char buf[MAX_LINE_LENGTH];
    while (fgets(buf, MAX_LINE_LENGTH, f_ptr) != NULL && i < LIMIT) {
        char city[100];
        int pos = parse_city(buf, city);

        float temp = parse_temp(pos + 1, buf);
        printf("%s", buf);
        printf("%s : %f\n", city, temp);
        i++;
    }

    printf("Successfully read %ld lines the file: %p\n", i, (void *)f_ptr);

    fclose(f_ptr);

    return 0;
}


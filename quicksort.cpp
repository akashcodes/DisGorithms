#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h>

char *int2bin(int i);
int bin2int(const char *str);
void gen_arr(int* arr, int n, int seed);
void print_array(int* arr, int len);
char* get_next_sublabel(char* label, int dim);
int pivoting(int len, int* arr, char* label, int dim);
int partition(int len, int* arr, int pivot);
int send_and_receive(int len, int** p_t_arr, int b_index, char* label, int dim);
void sequential_quicksort(int* arr, int len);

int main (int argc, char* argv[]) {
    int master_id = 0;
    int task_id;
    int task_quant;

    // just for master
    double start, end, cpu_time_used;

    int len = 6; // quantity of integers per process
    int* arr = malloc(len * sizeof(int));;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &task_id);
    MPI_Comm_size(MPI_COMM_WORLD, &task_quant);
    gen_arr(arr, len, task_id);

    // checking if task_quant is a power of 2.
    // from https://stackoverflow.com/questions/3638431/determine-if-an-int-is-a-power-of-2-or-not-in-a-single-line
    if ((task_quant & (task_quant - 1)) != 0) {
        printf("number of processors was not a power of two.");
        return 0;
    }

    int dim = (int) (log(task_quant) / log(2));
    char* label = malloc(dim * sizeof(char) + 1);
    label = int2bin(task_id);
    label += strlen(label) - dim;
    MPI_Barrier(MPI_COMM_WORLD);
    if (task_id == master_id) {
        printf("Master: starting the clock.\n");
        start = MPI_Wtime();
    }

    //actual algorithm is here
    for(dim = strlen(label); dim > 0; dim--){
        MPI_Barrier(MPI_COMM_WORLD);
        int pivot = pivoting(len, arr, label, dim);
        int b_index = partition(len, arr, pivot);
        MPI_Barrier(MPI_COMM_WORLD);
        len = send_and_receive(len, &arr, b_index, label, dim);
    }

    //print_array(arr, len);
    MPI_Barrier(MPI_COMM_WORLD);
    if (task_id == master_id) {
        printf("Master: ending the clock.\n");
        end = MPI_Wtime();
        cpu_time_used = end - start;
        printf("We took %f time to sort the array.\n", cpu_time_used);
    }

    sequential_quicksort(arr, len);

    // print in order
    //
    int token = 0;
    if (task_id == master_id)
    {
        print_array(arr, len);
        MPI_Send(&token, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
    } else {
        int buffer;
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_INT, &buffer);
        if (buffer == 1) {
            print_array(arr, len);
            MPI_Recv(&token, buffer, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            if (task_id + 1 != task_quant) {
                MPI_Send(&token, 1, MPI_INT, ++task_id, 0, MPI_COMM_WORLD);
            }
        }
    }

    MPI_Finalize();
    return 0;
}

void gen_arr(int* arr, int n, int seed)
{
    srand((unsigned int)time(NULL) + seed);
    int i;
    for (i = 0; i < n; i++)
    {
        double scaled = (double)rand()/RAND_MAX;
        arr[i] = (int) (100 - 0 +1)*scaled + 0;
    }
}


void print_array(int *arr, int len) // used for debugging
{
    int i;
    for (i = 0; i < len; i++)
    {
        printf("%d ", arr[i]);
    }
    printf("\n");
}


// From stackoverflow
// https://stackoverflow.com/questions/1024389/print-an-int-in-binary-representation-using-c
char * int2bin(int i)
{
    size_t bits = sizeof(int) * CHAR_BIT;
    char * str = malloc(bits + 1);
    if(!str) return NULL;
    str[bits] = 0;
    // type punning because signed shift is implementation-defined
    unsigned u = *(unsigned *)&i;
    for(; bits--; u >>= 1)
        str[bits] = u & 1 ? '1' : '0';
    return str;
}


int bin2int(const char *str)
{
  return (int) strtol(str, NULL, 2);
}


void sequential_quicksort(int *arr, int len)
{
    if (len < 2) return;

    int pivot = arr[len / 2];

    int i, j;
    for (i = 0, j = len - 1; ; i++, j--)
    {
        while (arr[i] < pivot) i++;
        while (arr[j] > pivot) j--;

        if (i >= j) break;
        // swap pivot in
        int temp = arr[i];
        arr[i]     = arr[j];
        arr[j]     = temp;
    }
    sequential_quicksort(arr, i);
    sequential_quicksort(arr + i, len - i);
}


int pivoting(int len, int* arr, char* label, int dim)
{
    MPI_Status status;
    // pivot step. Either generate the pivot and send it over, or receive it.
    int pivot;
    // find out if I need to generate the pivot
    int bool_gen_pivot = 1;
    int c;
    for (c = strlen(label) - dim; c < strlen(label); c++){
        if (label[c] != '0'){
            bool_gen_pivot = 0;
            break;
        }
    }

    if (bool_gen_pivot == 1) {
        if (len == 0) { pivot = 50;}
        else { pivot = arr[len - 1]; }

        //send it over to the right people
        char* next = malloc(strlen(label) * sizeof(char));
        strcpy(next, label);
        int j;
        for(j = 0; j < pow(2, dim) - 1; j++) {

            char* temp = get_next_sublabel(next, dim);
            free(next);
            next = temp;
            //printf("I am %d sending %d to %d\n", bin2int(label), pivot, bin2int(next));
            MPI_Send(&pivot, 1, MPI_INT, bin2int(next), 0, MPI_COMM_WORLD);
        }
        free(next);
    } else {
        char* temp = malloc(strlen(label) * sizeof(char));
        strcpy(temp, label);
        int j;
        for (j = strlen(label) - dim; j < strlen(label); j++){
            temp[j] = '0';
        }
        MPI_Recv(&pivot, 1, MPI_INT, bin2int(temp), 0, MPI_COMM_WORLD, &status);
        //printf("I am %d receiving %d from %d\n", bin2int(label), pivot, bin2int(temp));
        free(temp);
    }
    return pivot;
}

char* get_next_sublabel(char* label, int dim){
    char* next = malloc(dim * sizeof(label));
    strcpy(next, label);

    char* suffix = malloc(dim * sizeof(label));
    char* to_delete = suffix;
    int i;
    for(i = 0; i < dim; i++){
        suffix[i] = label[strlen(label) - dim + i];
    }

    int j = bin2int(suffix);
    j++;
    suffix = int2bin(j);
    suffix += strlen(suffix) - dim;

    for(i = 0; i < dim; i++){
        next[strlen(next) - dim + i] = suffix[i];
    }
    free(to_delete);
    return next;
}

int partition(int len, int* arr, int pivot)
{
    if (len == 0){ return 0; }
    // claim: [0,b) is <= than pivot, [b, n) is > than pivot.
    int a = 0;
    int b = len;
    int temp = 0;
    while(a != b)
    {
        if (arr[a] <= pivot) {
            a++;
        }
        else{
            temp = arr[b - 1];
            arr[b - 1] = arr[a];
            arr[a] = temp;
            b--;
        }
    }
    if (a == 0) { return 0; }
    return b;
}

int send_and_receive(int len, int** p_t_arr, int b_index, char* label, int dim){

    int* arr = *p_t_arr;
    MPI_Status status;
    // figure out if I send the higher or the lower arr.
    int receive_size;
    int send_size;
    int* receive;
    int* send;
    int* temp;
    char* contact_label = malloc(sizeof(char) * strlen(label));
    strcpy(contact_label, label);
    if(label[strlen(label) - dim] == '0')
    {
        //receive lower, send higher
        contact_label[strlen(label) - dim] = '1';

        MPI_Probe(bin2int(contact_label), 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_INT, &receive_size);
        receive = (int *)calloc(receive_size, sizeof(int));
        MPI_Recv(receive, receive_size, MPI_INT, bin2int(contact_label), 0,
                MPI_COMM_WORLD, &status);

        send_size = len - b_index;
        send = malloc(sizeof(int) * send_size);
        int i = 0;
        int c;
        for (c=len - 1; c > (len - send_size - 1); c--)
        {
            send[i] = arr[c];
            i++;
        }
        MPI_Send(send, send_size, MPI_INT, bin2int(contact_label), 0,
                MPI_COMM_WORLD);

        // make new array
        temp = malloc(sizeof(int) * (len - send_size + receive_size));

        for(i=0; i < len - send_size; i++) { temp[i] = arr[i]; }
        if (receive_size == 1 && receive[0] == -1) { receive_size = 0; }
        for(i=0; i < receive_size; i++) { temp[i + len - send_size] = receive[i]; }

    } else
    {
        //send lower, receive higher
        contact_label[strlen(label) - dim] = '0';
        send_size = b_index;

        if(send_size == 0) // send nothing
        {
            send_size = 1;
            send = malloc(sizeof(int));
            *send = -1;
            send_size = 0;
        } else
        {
            send = malloc(sizeof(int) * send_size + 1);
            int i;
            for (i=0; i < send_size; i++) { send[i] = arr[i]; }
        }
        MPI_Send(send, send_size, MPI_INT, bin2int(contact_label), 0,
                MPI_COMM_WORLD);

        MPI_Probe(bin2int(contact_label), 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_INT, &receive_size);
        receive = (int *)calloc(receive_size, sizeof(int));
        MPI_Recv(receive, receive_size, MPI_INT, bin2int(contact_label), 0,
                MPI_COMM_WORLD, &status);

        // make new array
        temp = malloc(sizeof(int) * (len - send_size + receive_size));
        int i;
        for(i=0; i < len - send_size; i++) { temp[i] = arr[i + send_size]; }
        if (receive_size == 1 && receive[0] == -1) { receive_size = 0; }
        for(i=0; i < receive_size; i++) { temp[i + len - send_size] = receive[i]; }
    }
    free(*p_t_arr); // for some reason doesnt work
    *p_t_arr = temp;
    free(send);
    free(receive);
    free(contact_label);

    return len - send_size + receive_size;
}

#define _POSIX_C_SOURCE 199309
#include <time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>


#define DEFAULT_SIZE 10000000
#define DEFAULT_THREADS 1

/*
 * TCSS 372 Hmk 5 Winter 2014 
 * Multi-threaded mergesort, using pre-supplied template
 * Dave Kaplan
 */
void multithreaded_mergesort(int A[], int size, int num_threads);

typedef struct {
	int* A;
	int left;
	int right;
} mergesort_arg;

typedef struct {
    int* A;
    int left1;
    int right1;
    int left2;
    int right2;
} merge_arg;

void* mergesort_wrapper(const mergesort_arg* arg);
void* merge_wrapper(const merge_arg* arg);
void mergesort(int A[], int left, int right);
void merge(int A[], int left1, int right1, int left2, int right2);


int ms_diff(struct timespec start, struct timespec stop);
void show_usage(const char* exe);


int main(int argc, char* argv[]) {
	int size = DEFAULT_SIZE;
	int num_threads = DEFAULT_THREADS;

	// Process command-line arguments.
	if (argc == 3) {
		char *end;
		size = strtol(argv[1], &end, 10);
		if (*end != 0) {
			show_usage(argv[0]);
			return EXIT_FAILURE;
		}
		num_threads = strtol(argv[2], &end, 10);
		if (*end != 0) {
			show_usage(argv[0]);
			return EXIT_FAILURE;
		}
		if (size < 1 || num_threads < 1) {
			show_usage(argv[0]);
			return EXIT_FAILURE;
		}
	} else if (argc != 1) {
		show_usage(argv[0]);
		return EXIT_FAILURE;
	}

	// Seed the random number generator.
	srand(time(NULL));

	// Allocate and populate an array.
	int* A = (int*)malloc(size * sizeof(int));
	if (A == NULL) {
		printf("ERROR: Unable to allocate array.\n");
		return EXIT_FAILURE;
	}
	for (int i = 0; i < size; i++)
		A[i] = rand();
	// Start tracking execution time.
	struct timespec start;
	clock_gettime(CLOCK_REALTIME, &start);

	multithreaded_mergesort(A, size, num_threads);

	// Stop tracking execution time.
	struct timespec stop;
	clock_gettime(CLOCK_REALTIME, &stop);
	printf("%d ms elapsed.\n", ms_diff(start, stop));
	free(A);
	return EXIT_SUCCESS;
}

void multithreaded_mergesort(int A[], int size, int num_threads) {
	// Conceptually partition the array and use separate threads to
	// independently sort each section.  The thread that invoked this
	// function will wait for all other threads to finish.

	// Create function arguments for all threads.
	mergesort_arg ms_args[num_threads];
    // these are for the cleanup merges if more than 1 thread
    merge_arg final_merges[num_threads];
	for (int i = 0; i < num_threads; i++) {
		ms_args[i].A = A;
		ms_args[i].left = i * size / num_threads;
		ms_args[i].right = (i + 1) * size / num_threads - 1;
	}

	// Create and start each thread.
	pthread_t threads[num_threads];
	for (int i = 0; i < num_threads; i++)
		if (pthread_create(&threads[i], NULL, mergesort_wrapper, &ms_args[i]) != 0) {
			printf("ERROR: Unable to create new thread.\n");
			exit(EXIT_FAILURE);
		}

	// Wait for all threads to finish.
	for (int i = 0; i < num_threads; i++)
		pthread_join(threads[i], NULL);

	// Perform "cleanup" work by merging remaining sections.

    // recursively merge thread's results from the bottom up,
    // doubling section size on each iteration.
    // create merge args for each step of the final merges
    // floor(log2(num_threads)) steps to reduce, plus potentially
    // one final merge with any remainder sections.
    
    int step = 2;  //inital span size, two halves of it are merged
    int count = 0; //keeps track of how many recursions for thread loop
    int j;
    while(step <= num_threads) {
        for (j =0; j < num_threads/step; j++) {
            final_merges[j].A = A;
            final_merges[j].left1 = ms_args[j*step].left;
            final_merges[j].right1 = ms_args[j*step+step/2-1].right;
            final_merges[j].left2 = ms_args[j*step+step/2].left;
            final_merges[j].right2 = ms_args[j*step+step-1].right;
            count++;
        }
        //params set up for thread loop and recursive steps counted
        //let threads work on final merges
        for(int i = 0; i < count; i++) {
            if (pthread_create(&threads[i], NULL, merge_wrapper, &final_merges[i]) != 0) {
			printf("ERROR: Unable to create new thread.\n");
			exit(EXIT_FAILURE);
            }
        }
        // Wait for all threads to finish.
        for (int i = 0; i < count; i++)
            pthread_join(threads[i], NULL);
        count = 0;    
        if(num_threads%step!=0) {
            //num_threads%step has a remainder! (num_threads!=2^n)
            //always merge the short tail remainder to the last
            //step sized chunk, once threads are synched.
            j--;
            merge(A, ms_args[j*step].left, ms_args[j*step+step-1].right,
            ms_args[j*step+step].left, ms_args[num_threads-1].right);
        }
        // double the step each time through the loop
        step*=2;
    }
}
void* merge_wrapper(const merge_arg* arg) {
	merge(arg->A, arg->left1, arg->right1, arg->left2, arg->right2);
	return NULL;
}

void* mergesort_wrapper(const mergesort_arg* arg) {
	fprintf(stderr, "Sorting section %d-%d.\n", arg->left, arg->right);
	mergesort(arg->A, arg->left, arg->right);
	return NULL;
}

// Indices left and right are inclusive.
void mergesort(int A[], int left, int right) {
    int mid;
    if(right - left < 1)  
        return;
    mid = left + (right - left) / 2;
    mergesort(A, left, mid);
    mergesort(A, mid + 1, right);
    merge(A, left, mid, mid + 1, right);
}

//[left1, right1], [left2, right2] specify inclusive indexes
//of two adjacent sorted sections of the array A.
//Merges to a temp array then overwrites the 
//two original sections in A.
void merge(int A[], int left1, int right1, int left2, int right2) {
    // the next unmerged element in the 1st subarray
    int subarray1_index = left1;
    // the next unmerged element in the 2nd subarray
    int subarray2_index = left2;
    // the next index in the temp array to be written to
    int temp_index = 0;
    // the length of the two adjacent sections
    int sub_size = right2 - left1 + 1;

    //alocate a temp array of size right2 - left1 + 1
    int* TEMP = (int*)malloc(sub_size * sizeof(int));
	if (TEMP == NULL) {
		printf("ERROR: Unable to allocate array.\n");
		return;
	}
    while (temp_index < sub_size) {
        if (subarray1_index > right1) {
            // if left sub-array used up, just take out of right array
            TEMP[temp_index++] = A[subarray2_index++];
        } else if (subarray2_index > right2) {
            // if right sub-array used up, just take out of left array
            TEMP[temp_index++] = A[subarray1_index++];
        } else if (A[subarray1_index] < A[subarray2_index])  {
            //the leftmost remaining element in left array is lowest
            TEMP[temp_index++] = A[subarray1_index++];
        } else {
            //the leftmost remaining element in right array is lowest
            TEMP[temp_index++] = A[subarray2_index++];
        }
    }
    //now copy TEMP to overwrite the original elements of A[]
    for (int i = 0; i < sub_size; i++) {
        A[left1 + i] = TEMP[i];
    }
    free(TEMP);
}

int ms_diff(struct timespec start, struct timespec stop) {
	return 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_nsec - start.tv_nsec) / 1000000;
}

void show_usage(const char* exe) {
	printf("Usage: %s [<array-size> <num-threads>]\n", exe);
	printf("  If omitted, <array-size> defaults to %d.\n", DEFAULT_SIZE);
	printf("  If omitted, <num-threads> defaults to %d.\n", DEFAULT_THREADS);
}


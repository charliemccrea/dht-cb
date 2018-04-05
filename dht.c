/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Bikash Adhikari, Charlie McCrea
 * Version: 2.0
 *
 */

#include <mpi.h>
#include <pthread.h>

#include "dht.h"

int hash(const char *name);

// Private module variable: current process ID
static int pid;

// PThreads
int remain;             // track number of remaining threads
pthread_mutex_t mutex;  // global mutex
pthread_cond_t condvar; // global condition variable

// MPI
int mpi_rank; // mpi process identifier
int mpi_size; // mpi processes total
int nprocs;   // number of mpi processes, not sure how diff from mpi_size...

// Other
int hash_owner;	// store result from hashing into table

/*
 * Initialize a new hash table. Returns the current process ID (always zero in
 * the serial version)
 *
 * (In the parallel version, this should spawn the server thread.)
 *
 * The server thread should execute a loop that waits for remote procedure call requests from  * other processes and delegates them to the appropriate local methods.
 * Each MPI process consists of two threads (client/main and server).
 * Because you are using threads in addition to MPI, you will want to initialize MPI using MP  * I_Init_thread() instead of MPI_Init()
 */
int dht_init()
{
	int provided;

	MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
	if (provided != MPI_THREAD_MULTIPLE)
	{
		printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
		exit(EXIT_FAILURE);
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

	/// while();

	//pthreadcreate points to a looping function that needs to be created

	//pid = getpid();
	if (pid == 0)
	{
		local_init();
	}
	else
	{

	}

	return pid;
}

/*
 * Save a key-value association. If the key already exists, the associated value
 * is changed in the hash table. If the key does not already exist, a new pair
 * is created with the given value.
 *
 * (In the parallel version, this should perform point-to-point MPI
 * communication as necessary to complete the DHT operation.)
 */
void dht_put(const char *key, long value)
{
	// if the pid is local, then keep it there, otherwise need to send it to the
	// right process via point-to-point MPI communication
	hash_owner = hash(key);
	if (hash_owner == pid)
	{
		local_put(key, value);
	}
	else
	{
		//mpi send to appropriate process
	}
}

/*
 * Retrieve a value given a key. If the key is found, the resulting value is
 * returned. Otherwise, the function should return KEY_NOT_FOUND.
 *
 * (In the parallel version, this should perform point-to-point MPI
 * communication as necessary to complete the DHT operation.)
 */
long dht_get(const char *key)
{
	return local_get(key);
}

/*
 * Returns the total size of the DHT.
 *
 * (In the parallel version, this should perform MPI communication as necessary
 * to complete the DHT operation.)
 */
size_t dht_size()
{
	return local_size();
}

/*
 * Synchronize all client processes involved in the DHT. This function should
 * not return until other all client processes have also called this function.
 *
 * (In the parallel version, this should essentially be a global barrier.)
 */
void dht_sync()
{
	/*
	// initialization
	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&condvar, NULL);

	// lock mutex and ensure safety of variable remain
	pthread_mutex_lock(&mutex);
	remain--;

	//last thread to enter wakes all sleeping threads
	if (remain == 0)
	{
		pthread_cond_broadcast(&condvar);
	}
	else
	{
		while(remain != 0)
		{
			pthread_cond_wait(&condvar, &mutex);
		}
	}
	pthread_mutex_unlock(&mutex);
	*/
}

/*
 * Given a key name, return the distributed hash table owner
 * (uses djb2 algorithm: http://www.cse.yorku.ca/~oz/hash.html)
 */
int hash(const char *name)
{
	unsigned hash = 5381;
	while (*name != '\0')
	{
		hash = ((hash << 5) + hash) + (unsigned)(*name++);
	}
	return hash % nprocs;
}

/*
 * Dump contents and clean up the hash table.
 *
 * (In the parallel version, this should terminate the server thread.)
 */
void dht_destroy(FILE *output)
{
	MPI_Finalize();
	local_destroy(output);
}

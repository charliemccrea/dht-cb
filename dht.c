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
#include <sys/types.h>
#include <unistd.h>

#include "dht.h"
#define MAX_LOCAL_PAIRS 65536

// Method declarations
int hash(const char *name);
void *server_thread(void *ptr);

// Variable declarations
static int pid;    // current proccess id
static int rank;   // current proccess rank
static int nprocs; // number of mpi processes
int hash_owner;	   // store result from hashing into table
bool alive = true;

/*
 * Private module structure: holds data for a single key-value pair
 */
struct kv_pair_dht
{
	char key[MAX_KEYLEN];
	long value;
	int type;
};

/*
 * Private module variable: array that stores all local key-value pairs
 *
 * NOTE: the pairs are stored lexicographically by key for cleaner output
 */
static struct kv_pair_dht kv_pairs_dht[MAX_LOCAL_PAIRS];

/*
 * Server thread
 */
void *server_thread(void *ptr)
{
	//pthreadcreate points to a looping MPI_Recv that needs to be created
	//MPI_Recv (void *buf, int count, MPI_Datatype dtype, int src, int tag, MPI_Comm comm, MPI_Status *status)
	//while(MPI_Recv(&recieve_key, sizeof, MPI_Byte, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status) == 0);
	//case type 1 would be to call dht_put, except it would be local this time

	MPI_Status status;
	struct kv_pair_dht receive_pair;

	while (alive)
	{
		printf("\npid = %d\n", getpid());
		printf("Pre receive\n");
		MPI_Recv(&receive_pair, sizeof(struct kv_pair_dht), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
		printf("Post receive\n");
		printf("receive_pair.type = %d", receive_pair.type);

		switch (receive_pair.type)
		{
			case 1: // put
				local_put(receive_pair.key, receive_pair.value);
				break;

			case 2: // get
				break;

			case 3: // sync
				break;

			case 4: // destroy
				alive = false;
				break;

			default:
				break;
		}
	}

	pthread_exit(0);
}

/*
 * Initialize a new hash table. Returns the current process ID (always zero in
 * the serial version)
 *
 * (In the parallel version, this should spawn the server thread.)
 */
int dht_init()
{
	int provided;
	pthread_t thread;

	MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
	if (provided != MPI_THREAD_MULTIPLE)
	{
		printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
		exit(EXIT_FAILURE);
	}

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

	if(rank == 0) memset(kv_pairs_dht, 0, sizeof(struct kv_pair_dht) * MAX_LOCAL_PAIRS);

	pthread_create(&thread, NULL, (void *)&server_thread, NULL);

	return rank;
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
	struct kv_pair_dht send_pair;

	if (hash_owner == getpid())
	{
		local_put(key, value);
	}
	else
	{
		sprintf(send_pair.key, "%s", key);
		send_pair.value = value;
		send_pair.type = 1;

		printf("\nPre send\n");
		MPI_Send (&send_pair, sizeof(struct kv_pair_dht), MPI_BYTE, hash_owner, 1, MPI_COMM_WORLD);
		printf("Post send\n");
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
	hash_owner = hash(key);
	if (hash_owner == pid)
        {
		return local_get(key);
        }
	else
	{

	}

	//placeholder
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
	local_destroy(output);
	MPI_Finalize();
}

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
static int rank;   // current proccess rank
static int nprocs; // number of mpi processes
int hash_owner;	   // store result from hashing into table
bool threads_online = true;

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
	MPI_Status status;
	struct kv_pair_dht receive_pair;
	long val;

	while(threads_online)
	{
		// Receive MPI sends to server thread
		printf("\npid = %d\n", getpid());
		printf("Pre receive\n");
		MPI_Recv(&receive_pair, sizeof(struct kv_pair_dht), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
		printf("Post receive\n");
		printf("receive_pair.type = %d", receive_pair.type);

		switch(receive_pair.type)
		{
			case 1: // put
				local_put(receive_pair.key, receive_pair.value);
				break;

			case 2: // get
				val = local_get(receive_pair.key);

				// Send value back to dht_get
				MPI_Send(&val, sizeof(long), MPI_BYTE, hash_owner, 2, MPI_COMM_WORLD);
				break;

			case 3: // sync
				break;

			case 4: // destroy
				threads_online = false;
				break;

			default:
				break;
		}
	}

	pthread_exit(0);
}

/*
 * Initialize a new hash table. Returns the current process ID. Spawns server thread.
 */
int dht_init()
{
	int provided;
	pthread_t thread;

	// Initialize thread
	MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
	if(provided != MPI_THREAD_MULTIPLE)
	{
		printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
		exit(EXIT_FAILURE);
	}
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

	// Intialize hash table
	if(rank == 0) memset(kv_pairs_dht, 0, sizeof(struct kv_pair_dht) * MAX_LOCAL_PAIRS);

	// Create server threads
	pthread_create(&thread, NULL, (void *)&server_thread, NULL);

	return rank;
}

/*
 * Save a key-value association. If the key already exists, the associated value
 * is changed in the hash table. If the key does not already exist, a new pair
 * is created with the given value. Perform MPI send to server thread.
 */
void dht_put(const char *key, long value)
{
	hash_owner = hash(key);
	struct kv_pair_dht send_pair;

	// If pid matches hash owner, make a local put
	if(hash_owner == getpid())
	{
		local_put(key, value);
	}
	else
	{
		// Prepare pair struct
		sprintf(send_pair.key, "%s", key);
		send_pair.value = value;
		send_pair.type = 1;

		// MPI send to server thread
		printf("\nPre send\n");
		MPI_Send(&send_pair, sizeof(struct kv_pair_dht), MPI_BYTE, hash_owner, 0, MPI_COMM_WORLD);
		printf("Post send\n");
	}
}

/*
 * Retrieve a value given a key. If the key is found, the resulting value is
 * returned. Otherwise, the function should return KEY_NOT_FOUND. Perform MPI send and receive
 * to communicate with server thread.
 */
long dht_get(const char *key)
{
	MPI_Status status;
	hash_owner = hash(key);
	struct kv_pair_dht receive_pair;
	long val = -1;

	// If pid matches hash owner, make a local get
	if(hash_owner == getpid())
	{
		return local_get(key);
	}
	else
	{
		// Prepare pair struct
		sprintf(receive_pair.key, "%s", key);
		receive_pair.type = 2;

		// MPI send to server thread
		MPI_Send(&receive_pair, sizeof(struct kv_pair_dht), MPI_BYTE, hash_owner, 0, MPI_COMM_WORLD);

		// MPI receive value
		MPI_Recv(&val, sizeof(long), MPI_BYTE, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status);
	}

	// Return value
	if(val == -1)
	{
		return KEY_NOT_FOUND;
	}
	else
	{
		return val;
	}
}

/*
 * Returns the total size of the DHT.
 */
size_t dht_size()
{
	return local_size();
}

/*
 * Synchronize all client processes involved in the DHT. This function should
 * not return until other all client processes have also called this function.
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
	while(*name != '\0')
	{
		hash = ((hash << 5) + hash) + (unsigned)(*name++);
	}
	return hash % nprocs;
}

/*
 * Dump contents and clean up the hash table. Terminate the server thread.
 */
void dht_destroy(FILE *output)
{
	local_destroy(output);
	threads_online = false;
	MPI_Finalize();
}

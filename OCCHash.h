/*
 * Optimistic Concurrent Cuckoo Hashing
 *
 *  Created on: Oct 26, 2017
 *      Author: hhx008
 */

#ifndef _OCCHASH_H_
#define _OCCHASH_H_

# include <cstring>
# include <atomic>
# include <pthread.h>
# include <unistd.h>
# include "types.h"
# include "object.h"

# define SLOT_NUM 4
# define TABLE_INIT_SIZE (1 << 21)
# define MAXIMUM_DISPLACE 500
# define VERSION_COUNTER_SIZE 8192
# define OPT_LOCK_SLEEP_TIME 10

typedef unsigned char Tag;
typedef unsigned int Hash;

struct slot {
	bool commit;
	Tag tag;
	NVMP objp;
};

struct bucket {
	slot slots[SLOT_NUM]; // SLOT_NUM * 10 bytes
};

// TODO consistency (ensure atomicity of hash table operations)
class OCCHash {
private:
	pthread_mutex_t writelock;

	unsigned int bucketSize;
	unsigned int capacity;
	std::atomic<unsigned int> size;
	std::atomic<unsigned int> counter[VERSION_COUNTER_SIZE];
	bucket* table;

	object* locate(KStr key, bucket* & b, int & slot_idx);
	void kickout(int pathlen, slot* path[MAXIMUM_DISPLACE], Tag tag, object* objp);
	int pathSearch(Hash hash, Hash hash2, slot* path[MAXIMUM_DISPLACE]);
	int pathSearchHelper(Hash hash, int slot_idx, int depth, slot* path[MAXIMUM_DISPLACE]);
public:
	OCCHash() {
		pthread_mutex_init(&writelock, NULL);

		size = 0;
		capacity = TABLE_INIT_SIZE;
		bucketSize = capacity / SLOT_NUM;
		table = NULL;
		for (int i=0; i<VERSION_COUNTER_SIZE; i++) {
			counter[i].store(0);
		}
	}
	// DOES NOT free objp here, since OCCHash does not allocate any KV
	~OCCHash() {
		pthread_mutex_destroy(&writelock);
	}

	void init(int _size, bucket* _table) { size = _size; table = _table; };
	int Size();
	bool Update(KStr key, object* objp);
	bool Insert(KStr key, object* objp);
	object* Lookup(KStr key);
	object* Delete(KStr key);
};

struct cacheSlot {
	Tag tag;
	object* objp;
};

struct cacheBucket {
	cacheSlot slots[SLOT_NUM]; // SLOT_NUM * 9 bytes
};

class OCCHashCache {
private:
	pthread_mutex_t writelock;

	unsigned int bucketSize;
	unsigned int capacity;
	std::atomic<unsigned int> size;
	std::atomic<unsigned int> counter[VERSION_COUNTER_SIZE];
	cacheBucket* table;

	object* locate(KStr key, cacheBucket* & b, int & slot_idx);
	void kickout(int pathlen, cacheSlot* path[MAXIMUM_DISPLACE], Tag tag, object* objp);
	int pathSearch(Hash hash, Hash hash2, cacheSlot* path[MAXIMUM_DISPLACE]);
	int pathSearchHelper(Hash hash, int slot_idx, int depth, cacheSlot* path[MAXIMUM_DISPLACE]);
public:
	OCCHashCache() {
		pthread_mutex_init(&writelock, NULL);

		capacity = TABLE_INIT_SIZE;
		bucketSize = capacity / SLOT_NUM;
		table = NULL;
		for (int i=0; i<VERSION_COUNTER_SIZE; i++) {
			counter[i].store(0);
		}
	}
	// DOES NOT free objp here, since OCCHash does not allocate any KV
	~OCCHashCache() {
		pthread_mutex_destroy(&writelock);
		delete table;
	}

	void init() { size = 0; table = new cacheBucket[bucketSize];
				memset(table, 0, bucketSize * sizeof(cacheBucket)); };
	int Size();
	bool Update(KStr key, object* objp);
	bool Insert(KStr key, object* objp);
	object* Lookup(KStr key);
	object* Delete(KStr key);
};

#endif

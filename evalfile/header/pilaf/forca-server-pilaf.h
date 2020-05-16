/*
 * forca-server-pilaf.h
 *
 *  Created on: May 22, 2018
 *      Author: hhx008
 */

#ifndef FORCA_SERVER_PILAF_H_
#define FORCA_SERVER_PILAF_H_

# include <cstring>
# include <atomic>
# include <pthread.h>
# include <unistd.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <sys/fcntl.h>
# include <libpmem.h>
# include <sys/mman.h>
# include "types.h"
# include "const.h"
# include "allocator.h"
# include "network.h"
# include "lock.h"

# define SLOT_NUM 1
# define TABLE_INIT_SIZE (1 << 17)
# define MAXIMUM_DISPLACE 500
# define VERSION_COUNTER_SIZE 8192
# define OPT_LOCK_SLEEP_TIME 10

# define REGIONBLOCK_NUM (1UL << 23)
# define NVMSIM_PATH "data/nvm.sim"
# define NVM_SIZE (TABLE_INIT_SIZE * sizeof(pilafBucket) + REGIONBLOCK_NUM * REGIONBLOCK_SIZE)

inline void persist(const void * addr, int len) {
	// if (pmem_is_pmem(addr, len))
		pmem_persist(addr, len);
	// else
		// pmem_msync(addr, len);
	// msync(addr, len, MS_SYNC);
}

struct pilafSlot {
	Tag tag;
	KStr key;
	char* vp;
	int valuesize;
	unsigned int kv_crc32;
	unsigned int slot_crc32;
};
struct pilafBucket {
	pilafSlot slots[SLOT_NUM];
};

class OCCHashPilaf {
private:
	pthread_mutex_t writelock;

	unsigned int bucketSize;
	unsigned int capacity;
	std::atomic<unsigned int> size;
	std::atomic<unsigned int> counter[VERSION_COUNTER_SIZE];
	pilafBucket* table;

	char* locate(KStr key, pilafBucket* & b, int & slot_idx);
	void kickout(int pathlen, pilafSlot* path[MAXIMUM_DISPLACE], Tag tag, KStr key, char* vp,
			int vsize, unsigned int kv_crc32, unsigned int slot_crc32);
	int pathSearch(Hash hash, Hash hash2, pilafSlot* path[MAXIMUM_DISPLACE]);
	int pathSearchHelper(Hash hash, int slot_idx, int depth, pilafSlot* path[MAXIMUM_DISPLACE]);
public:
	OCCHashPilaf() {
		pthread_mutex_init(&writelock, NULL);

		table = NULL;
		for (int i=0; i<VERSION_COUNTER_SIZE; i++) {
			counter[i].store(0);
		}
	}
	~OCCHashPilaf() {
		pthread_mutex_destroy(&writelock);
	}

	void init(pilafBucket * bucket) {
		capacity = TABLE_INIT_SIZE;
		bucketSize = capacity / SLOT_NUM;
		size = 0;
		table = bucket;
		memset(table, 0, bucketSize * sizeof(pilafBucket));
		for (int i=0; i<bucketSize; i++) {
			for (int j=0; j<SLOT_NUM; j++)
				table[i].slots[j].slot_crc32 = calSlotCRC32(table[i].slots[j].tag, table[i].slots[j].key, table[i].slots[j].vp, table[i].slots[j].valuesize, table[i].slots[j].kv_crc32);
		}
	};
	int Size();
	bool Update(KStr key, char* vp, int vsize, unsigned int kv_crc32);
	bool Insert(KStr key, char* vp, int vsize, unsigned int kv_crc32);
	char* Lookup(KStr key);
	char* Delete(KStr key);
};

// implement this class to make your own server
// things to do: implement run (should be a loop); implement msg handler; etc.
// NOTE: one run thread pairs with one client (i.e one RDMASocket)
// notes: ensure parameters passed to client & server are the same
class ForcaServer {

# define RDMAdo(X) rsocketv[rid]->X
# define DBdo(X) db.X

protected:
	struct RunParams {
		ForcaServer * fcs;
		int rid;
		RunParams(ForcaServer * f, int i) : fcs(f), rid(i) {};
	};

	const int netMsgSize;
	const int maxSizePerOp;

	OCCHashPilaf hash;
	void * nvmstart;
	char * hashregion;
	char * kvregion;

	pthread_spinlock_t regionpoollock;
	BlockAllocator kvrAllocator;

	RDMACoordinator coordinator;
	std::vector<RDMASocket*> rsocketv;
	std::vector<pthread_t> rthreadv;

	// a function for handling outstanding client request
	void outstandHandlerLoop() {
		pthread_t trthread;
		while (true) {
			while (!coordinator.outstanding) {
				usleep(100000);
			}

			std::cout << "we got outstanding!" << std::endl;
			RDMASocket * nrsocket = new RDMASocket(true, netMsgSize, maxSizePerOp);
			nrsocket->init((void *)nvmstart, NVM_SIZE);

			rsocketv.push_back(nrsocket);
			coordinator.messenger = nrsocket;
			coordinator.outstanding = false;
			// wait for connection finishes
			std::cout << "wait for connection done..." << std::endl;
			while (coordinator.connecting) {
				usleep(1000);
			}
			std::cout << "connection done, prepostRecv()" << std::endl;
			nrsocket->prepostRecv();

			RunParams * rp = new RunParams(this, rsocketv.size()-1);
			pthread_create(&trthread, NULL, runRoutine, (void*)rp);
			pthread_detach(trthread);
		}
	};
	static void * runRoutine(void * args) {
		RunParams * rp = (RunParams *) args;
		ForcaServer * fcs = rp->fcs;
		int rid = rp->rid;
		delete rp;

		fcs->run(rid);
		return NULL;
	};
public:
	ForcaServer(int nms, int mspo) : netMsgSize(nms), maxSizePerOp(mspo), kvrAllocator(REGION_ALLOCPOWER, REGIONBLOCK_NUM), coordinator(true) {
		size_t mapped_len;
		int is_pmem;
		int fd = open(NVMSIM_PATH, O_RDWR, 0);
		if (fd < 0)
			std::cout << "open nvm.sim failed" << std::endl;
		nvmstart = pmem_map_file(NVMSIM_PATH, NVM_SIZE, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
		assert(nvmstart);
		close(fd);
		std::cout << "NVMSTART: " << nvmstart << std::endl;

		void* tmp = nvmstart;
		hashregion = (char *)tmp;
		std::cout << "hash region: " << (void *)hashregion << std::endl;
		tmp += (TABLE_INIT_SIZE / SLOT_NUM) * sizeof(pilafBucket);
		kvregion = (char *)tmp;
		std::cout << "kv region: " << (void *)kvregion << std::endl;
		hash.init((pilafBucket *)hashregion);
		pthread_spin_init(&regionpoollock, PTHREAD_PROCESS_PRIVATE);
		kvrAllocator.initFreelist();
		std::cout << "let's roll" << std::endl;
	}
	virtual ~ForcaServer() {
		for (auto rsocket : rsocketv)
			delete rsocket;
		pthread_spin_destroy(&regionpoollock);
	};
	void start() {
		coordinator.start();
		// start outstanding handler
		outstandHandlerLoop();
	}
	void run(int rid) {
		std::cout << "run() starts" << std::endl;

		int msgtype, wrid;

		while (true) {
			wrid = RDMAdo(recvMsg(msgtype, NULL, true));
			char * sendbuf = RDMAdo(getSendBuf());
			char * recvbuf = RDMAdo(getRecvBuf());
			switch (msgtype) {
			case INTERNAL_CLIENT_EXIT:
				std::cout << "client " << rid << " exits" << std::endl;
				delete rsocketv[rid];
				rsocketv[rid] = NULL;
				return;
			case INTERNEL_CLIENT_GETBASEADDR:
				std::cout << "client " << rid << " wants remote base addr" << std::endl;
				*(unsigned long long *)sendbuf = (unsigned long long)hashregion;
				RDMAdo(sendMsg(0, NULL, true));
				break;
			default:
				process(rid, msgtype, recvbuf);
			}
		}
	}
	virtual void process(int rid, int msgtype, char recvbuf[]) = 0;

	char * allocateRegion(int power) {
		int blkid = kvrAllocator.allocate(power);
		if (blkid < 0)
			return NULL;
		return kvregion + blkid * REGIONBLOCK_SIZE;
	}

	bool freeRegion(char * region) {
		int blkid = (region - kvregion) / REGIONBLOCK_SIZE;
		kvrAllocator.free(blkid);
		return true;
	}
};


/* ensure lock protection before call, it's protected
 * by optimistic lock in Insert(), Lookup() and mutex in Delete()
 * assign result bucket and slot index to b & slot_idx
 */
char* OCCHashPilaf::locate(KStr key, pilafBucket* & b, int & slot_idx) {
	Hash hash = FNV1Ahash(key) % bucketSize;
	Tag tag = TAGhash(key);
	KStr str;
	itos(tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	char* fvp = NULL;
	b = &table[hash];
	// check first bucket
	for (int i=0; i<SLOT_NUM; i++) {
		char* vp = b->slots[i].vp;
		if (vp == NULL)
			continue;
		if (b->slots[i].tag == tag) {
			if (strcmp(b->slots[i].key, key) == 0) {
				slot_idx = i;
				fvp = vp;
				break;
			}
		}
	}
	// check the other bucket
	if (fvp == NULL) {
		b = &table[hash2];
		for (int i=0; i<SLOT_NUM; i++) {
			char* vp = b->slots[i].vp;
			if (vp == NULL)
				continue;
			if (b->slots[i].tag == tag) {
				if (strcmp(b->slots[i].key, key) == 0) {
					slot_idx = i;
					fvp = vp;
					break;
				}
			}
		}
	}

	return fvp;
}

/* ensure lock protection before call, it's protected
 * by optimistic lock & mutex in Insert() and Resize()
 */
void OCCHashPilaf::kickout(int pathlen, pilafSlot* path[MAXIMUM_DISPLACE], Tag tag, KStr key, char* vp,
		int vsize, unsigned int kv_crc32, unsigned int slot_crc32) {
	// reverse kick-out
	for (int i=1; i<pathlen; i++) {
		pilafSlot* sprev = path[i-1];
		pilafSlot* scurr = path[i];
		sprev->tag = scurr->tag;
		sprev->vp = scurr->vp;
		sprev->valuesize = scurr->valuesize;
		sprev->kv_crc32 = scurr->kv_crc32;
		sprev->slot_crc32 = scurr->slot_crc32;
		memcpy(sprev->key, scurr->key, OBJECT_KEY_LEN);
	}
	
	
	path[pathlen-1]->tag = tag;
	path[pathlen-1]->vp = vp;
	path[pathlen-1]->valuesize = vsize;
	path[pathlen-1]->kv_crc32 = kv_crc32;
	path[pathlen-1]->slot_crc32 = slot_crc32;
	memcpy(path[pathlen-1]->key, key, OBJECT_KEY_LEN);
}

int OCCHashPilaf::pathSearch(Hash hash, Hash hash2, pilafSlot* path[MAXIMUM_DISPLACE]) {
	int pathidx, tmplen, pathlen = MAXIMUM_DISPLACE + 1;
	pilafSlot* pathset[2*SLOT_NUM][MAXIMUM_DISPLACE];

	for (int i=0; i<2*SLOT_NUM; i++)
		for (int j=0; j<MAXIMUM_DISPLACE; j++) {
			pathset[i][j] = NULL;
		}

	for (int i=0; i<SLOT_NUM; i++) {
		tmplen = pathSearchHelper(hash, i, 1, pathset[i]);
		if (tmplen < pathlen) {
			pathlen = tmplen;
			pathidx = i;
		}
	}
	for (int i=0; i<SLOT_NUM; i++) {
		tmplen = pathSearchHelper(hash2, i, 1, pathset[SLOT_NUM + i]);
		if (tmplen < pathlen) {
			pathlen = tmplen;
			pathidx = SLOT_NUM + i;
		}
	}

	if (pathlen < MAXIMUM_DISPLACE + 1) {
		for (int i=0; i<pathlen; i++)
			path[i] = pathset[pathidx][i];
	}

	return pathlen;
}

int OCCHashPilaf::pathSearchHelper(Hash hash, int slot_idx, int depth, pilafSlot* path[MAXIMUM_DISPLACE]) {
	if (depth > MAXIMUM_DISPLACE)
		return MAXIMUM_DISPLACE + 1;

	pilafSlot* s = &table[hash].slots[slot_idx];
	KStr str;
	itos(s->tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	pilafBucket* b = &table[hash2];
	for (int i=0; i<SLOT_NUM; i++) {
		if (b->slots[i].vp == NULL) {
			path[0] = &b->slots[i];
			path[1] = s;
			return 2;
		}
	}
	// to kick random one out
	int len = pathSearchHelper(hash2, s->tag % SLOT_NUM, depth+1, path);
	if (len < MAXIMUM_DISPLACE) {
		path[len] = s;
		return len + 1;
	} else return MAXIMUM_DISPLACE + 1;
}

int OCCHashPilaf::Size() {
	int s = size.load();
	return s;
}

bool OCCHashPilaf::Update(KStr key, char* vp, int vsize, unsigned int kv_crc32) {
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	Tag tag = TAGhash(key);
	char* tvp = NULL;
	pilafBucket* b;
	int vc, newvc, slot_idx;

	do {
		vc = counter[hash].load();
		if (vc % 2) {
			// someone's modifying it, set newvc != vc
			newvc = vc + 1;
			continue;
		}

		tvp = locate(key, b, slot_idx);
		newvc = counter[hash].load();
	} while (newvc != vc);

	if (tvp == NULL)
		return false;

	pthread_mutex_lock(&writelock);
	// std::cout << "UPDATE" << std::endl;
	counter[hash]++;
	b->slots[slot_idx].tag = tag;
	b->slots[slot_idx].vp = vp;
	b->slots[slot_idx].valuesize = vsize;
	b->slots[slot_idx].kv_crc32 = kv_crc32;
	memcpy(b->slots[slot_idx].key, key, OBJECT_KEY_LEN);
	b->slots[slot_idx].slot_crc32 = calSlotCRC32(tag, key, vp, vsize, kv_crc32);
	counter[hash]++;
	pthread_mutex_unlock(&writelock);

	return true;
}

// TODO, how many cuckoo paths is best?
bool OCCHashPilaf::Insert(KStr key, char* vp, int vsize, unsigned int kv_crc32) {
	if (Lookup(key) != NULL)
		return true;

	pthread_mutex_lock(&writelock);
	Hash counterhash = APHash(key) % VERSION_COUNTER_SIZE;
	Hash hash = FNV1Ahash(key) % bucketSize;
	Tag tag = TAGhash(key);
	KStr str;
	itos(tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);
	// std::cout << "INSERT key: " << key << ", hash: " << hash << ", hash2: " << hash2 << std::endl;

	int slot_idx = -1;
	pilafBucket* b = &table[hash];
	for (int i=0; i<SLOT_NUM; i++) {
		if (b->slots[i].vp == NULL) {
			slot_idx = i;
			break;
		}
	}
	if (slot_idx < 0) {
		b = &table[hash2];
		for (int i=0; i<SLOT_NUM; i++) {
			if (b->slots[i].vp == NULL) {
				slot_idx = i;
				break;
			}
		}
	}
	int slot_crc32 = calSlotCRC32(tag, key, vp, vsize, kv_crc32);
	// std::cout << "INSERT key: " << key << std::endl;

	if (slot_idx >= 0) {
		counter[counterhash]++;
		b->slots[slot_idx].tag = tag;
		b->slots[slot_idx].vp = vp;
		b->slots[slot_idx].valuesize = vsize;
		b->slots[slot_idx].kv_crc32 = kv_crc32;
		memcpy(b->slots[slot_idx].key, key, OBJECT_KEY_LEN);
		b->slots[slot_idx].slot_crc32 = slot_crc32;
		// std::cout << "insert: " << b->slots[slot_idx].kv_crc32 << ", " << b->slots[slot_idx].slot_crc32 << std::endl;
		size++;
		counter[counterhash]++;

		pthread_mutex_unlock(&writelock);
		return true;
	}

	pilafSlot* targetpath[MAXIMUM_DISPLACE];
	int pathlen = pathSearch(hash, hash2, targetpath);
	if (pathlen < MAXIMUM_DISPLACE + 1) {
		counter[counterhash]++;
		kickout(pathlen, targetpath, tag, key, vp, vsize, kv_crc32, slot_crc32);
		size++;
		counter[counterhash]++;
	} else {
		pthread_mutex_unlock(&writelock);
		return false;
	}

	pthread_mutex_unlock(&writelock);
	return true;
}

char* OCCHashPilaf::Lookup(KStr key) {
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	char* vp = NULL;
	pilafBucket* b;
	int vc, newvc, slot_idx;

	do {
		vc = counter[hash].load();
		if (vc % 2) {
			// someone's modifying it, set newvc != vc
			newvc = vc + 1;
			continue;
		}

		vp = locate(key, b, slot_idx);
		newvc = counter[hash].load();
	} while (newvc != vc);

	return vp;
}

char* OCCHashPilaf::Delete(KStr key) {
	pthread_mutex_lock(&writelock);
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	pilafBucket* b;
	int slot_idx;

	char* vp = locate(key, b, slot_idx);
	if (vp != NULL) {
		counter[hash]++;
		size--;
		b->slots[slot_idx].vp = NULL;
		b->slots[slot_idx].slot_crc32 = calSlotCRC32(b->slots[slot_idx].tag, b->slots[slot_idx].key,
				b->slots[slot_idx].vp, b->slots[slot_idx].valuesize, b->slots[slot_idx].kv_crc32);
		counter[hash]++;
	}

	pthread_mutex_unlock(&writelock);
	return vp;
}

#endif /* FORCA_SERVER_PILAF_H_ */

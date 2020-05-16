/*
 * nvm.h
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

#ifndef NVM_H_
#define NVM_H_

# include <pthread.h>
# include <libpmem.h>
# include <sys/mman.h>
# include "const.h"
# include "types.h"
# include "object.h"
# include "allocator.h"
# include "assert.h"

# define OBJECT_NUM (1UL << 20)
# define METABLOCK_NUM (1UL << 23)
# define DATABLOCK_NUM (1UL << 23)
# define REGIONBLOCK_NUM (1UL << 25)
# define OBJECT_POOL_SIZE (OBJECT_NUM * sizeof(object))
# define META_POOL_SIZE (METABLOCK_NUM * sizeof(metablock))
# define DATA_POOL_SIZE (DATABLOCK_NUM * sizeof(datablock))
# define REGION_POOL_SIZE (REGIONBLOCK_NUM * REGIONBLOCK_SIZE)

# define NVM_SIZE (OBJECT_POOL_SIZE + META_POOL_SIZE + DATA_POOL_SIZE + REGION_POOL_SIZE)

inline void persist(const void * addr, int len) {
	// if (pmem_is_pmem(addr, len))
		pmem_persist(addr, len);
	// else
		// pmem_msync(addr, len);
	// msync(addr, len, MS_SYNC);
}

class NVM {
	// data block allocation node
private:
	pthread_spinlock_t objpoollock;
	BlockAllocator objAllocator;
	pthread_spinlock_t datapoollock;
	BlockAllocator dataAllocator;
	pthread_spinlock_t metapoollock;
	BlockAllocator metaAllocator;

	pthread_spinlock_t regionpoollock;
	BlockAllocator regionAllocator;
	void* nvmstart;
	object* objpool;
	metablock* metapool;
	datablock* datapool;
	char* regionpool;

	void initLock();
	void destroyLock();
public:
	NVM() : objAllocator(OBJECT_ALLOCPOWER, OBJECT_NUM), dataAllocator(DATA_ALLOCPOWER, DATABLOCK_NUM),
				metaAllocator(META_ALLOCPOWER, METABLOCK_NUM), regionAllocator(REGION_ALLOCPOWER, REGIONBLOCK_NUM) {
		nvmstart = NULL;
		objpool = NULL;
		metapool = NULL;
		datapool = NULL;
		regionpool = NULL;
	};
	~NVM();

	void init();
	void initFreelist();
	void markObjUsed(object * obj);
	object * allocateObject();
	bool freeObject(object * obj);
	unsigned long long getObjectUsage() { return objAllocator.getUsage(); };
	void markMetaUsed(metablock * blk);
	metablock* allocateMetaBlock();
	bool freeMetaBlock(metablock * blk);
	unsigned long long getMetaUsage() { return metaAllocator.getUsage(); };

	void markDataUsed(datablock * blk);
	datablock* allocateDataBlock();
	bool freeDataBlock(datablock * blk);
	unsigned long long getDataUsage() { return dataAllocator.getUsage(); };

	void markRegionUsed(char * region, int power);
	char * allocateRegion(int power);
	bool freeRegion(char * region);
	unsigned long long getRegionUsage() { return regionAllocator.getUsage(); };

	inline void* getNVMStart() { return nvmstart; };
	inline object* getObjPool() { return objpool; };
	inline metablock* getMetaPool() { return metapool; };
	inline datablock* getDataPool() { return datapool; };
	inline char* getRegionPool() { return regionpool; };
	inline void* NVMPtoP(NVMP nvmp) { return nvmp == NULL ? NULL : (void *)(nvmstart + nvmp); };
	inline NVMP PtoNVMP(void * p) { return p == NULL ? NULL : (NVMP)((char *)p - (char *)nvmstart); };
};

#endif /* NVM_H_ */

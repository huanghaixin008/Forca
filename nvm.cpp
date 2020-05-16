/*
 * nvm.cpp
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

# include <iostream>
# include <sys/mman.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include "nvm.h"
# include "util.h"
# include "OCCHash.h"

# define NVMSIM_PATH "data/nvm.sim"

NVM::~NVM() {
	munmap(nvmstart, NVM_SIZE);
	destroyLock();
}

void NVM::init() {
	size_t mapped_len;
	int is_pmem;
	int fd = open(NVMSIM_PATH, O_RDWR, 0);
	if (fd < 0)
		std::cout << "open nvm.sim failed" << std::endl;
	nvmstart =  pmem_map_file(NVMSIM_PATH, NVM_SIZE, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
	assert(nvmstart);
	close(fd);
	std::cout << "NVMSTART: " << nvmstart << std::endl;

	void* tmp = nvmstart;
	objpool = (object*)tmp;
	tmp += OBJECT_POOL_SIZE;
	std::cout << "OBJPOOL: " << objpool << std::endl;
	metapool = (metablock*)tmp;
	tmp += META_POOL_SIZE;
	std::cout << "METAPOOL: " << metapool << std::endl;
	datapool = (datablock*)tmp;
	tmp += DATA_POOL_SIZE;
	std::cout << "DATAPOOL: " << datapool << std::endl;
	regionpool = (char*)tmp;
	std::cout << "REGIONPOOL: " << (void *)regionpool << std::endl;
	std::cout << "regionpool size: " << REGION_POOL_SIZE << std::endl;

	initLock();
	std::cout << "NVM init finished" << std::endl;
}

void NVM::initLock() {
	pthread_spin_init(&objpoollock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&metapoollock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&datapoollock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&regionpoollock, PTHREAD_PROCESS_PRIVATE);
}

void NVM::initFreelist() {
	objAllocator.initFreelist();
	dataAllocator.initFreelist();
	metaAllocator.initFreelist();
	regionAllocator.initFreelist();
}

void NVM::destroyLock() {
	pthread_spin_destroy(&objpoollock);
	pthread_spin_destroy(&metapoollock);
	pthread_spin_destroy(&datapoollock);
	pthread_spin_destroy(&regionpoollock);
}

void NVM::markObjUsed(object * obj) {
	int objid = obj - objpool;
	pthread_spin_lock(&objpoollock);
	objAllocator.markUsed(objid, OBJECT_ALLOCPOWER);
	pthread_spin_unlock(&objpoollock);
}

object* NVM::allocateObject() {
	pthread_spin_lock(&objpoollock);
	int objid = objAllocator.allocate(OBJECT_ALLOCPOWER);
	if (objid < 0)
		return NULL;
	pthread_spin_unlock(&objpoollock);
	// memset(objpool + objid, 0, sizeof(object));
	// persist();
	return objpool + objid;
}

bool NVM::freeObject(object * obj) {
	pthread_spin_lock(&objpoollock);
	objAllocator.free(obj - objpool);
	pthread_spin_unlock(&objpoollock);
	return true;
}

void NVM::markMetaUsed(metablock * blk) {
	int blkid = blk - metapool;
	pthread_spin_lock(&metapoollock);
	metaAllocator.markUsed(blkid, META_ALLOCPOWER);
	pthread_spin_unlock(&metapoollock);
}

metablock* NVM::allocateMetaBlock() {
	pthread_spin_lock(&metapoollock);
	int blkid = metaAllocator.allocate(META_ALLOCPOWER);
	if (blkid < 0)
		return NULL;
	pthread_spin_unlock(&metapoollock);
	// memset(metapool + blkid, 0, sizeof(metablock));
	// persist();
	return metapool + blkid;
}

bool NVM::freeMetaBlock(metablock * blk) {
	pthread_spin_lock(&metapoollock);
	metaAllocator.free(blk - metapool);
	pthread_spin_unlock(&metapoollock);
	return true;
}

void NVM::markDataUsed(datablock * blk) {
	int blkid = blk - datapool;
	pthread_spin_lock(&datapoollock);
	dataAllocator.markUsed(blkid, DATA_ALLOCPOWER);
	pthread_spin_unlock(&datapoollock);
}

datablock* NVM::allocateDataBlock() {
	pthread_spin_lock(&datapoollock);
	int blkid = dataAllocator.allocate(DATA_ALLOCPOWER);
	if (blkid < 0)
		return NULL;
	pthread_spin_unlock(&datapoollock);
	return datapool + blkid;
}

bool NVM::freeDataBlock(datablock * blk) {
	pthread_spin_lock(&datapoollock);
	dataAllocator.free(blk - datapool);
	pthread_spin_unlock(&datapoollock);
	return true;
}

void NVM::markRegionUsed(char * region, int power) {
	int blkid = (region - regionpool) / REGIONBLOCK_SIZE;
	pthread_spin_lock(&regionpoollock);
	regionAllocator.markUsed(blkid, power);
	pthread_spin_unlock(&regionpoollock);
}

char * NVM::allocateRegion(int power) {
	pthread_spin_lock(&regionpoollock);
	int blkid = regionAllocator.allocate(power);
	if (blkid < 0)
		return NULL;
	pthread_spin_unlock(&regionpoollock);
	// memset(datapool + blkid, 0, sizeof(datablock));
	// persist();
	return regionpool + blkid * REGIONBLOCK_SIZE;
}

bool NVM::freeRegion(char * region) {
	int blkid = (region - regionpool) / REGIONBLOCK_SIZE;
	pthread_spin_lock(&regionpoollock);
	regionAllocator.free(blkid);
	pthread_spin_unlock(&regionpoollock);
	return true;
}

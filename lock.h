/*
 * lock.h
 *
 *  Created on: Jan 26, 2018
 *      Author: hhx008
 */

#ifndef LOCK_H_
#define LOCK_H_

# include <atomic>
# include <iostream>
# include <unordered_map>
# include <unordered_set>
# include <pthread.h>
# include "types.h"
# include "object.h"

class ObjectIDRWLock {
private:
	const int objectNum;
	pthread_rwlock_t * rwlock;
public:
	ObjectIDRWLock(int objnum) : objectNum(objnum) {
		rwlock = new pthread_rwlock_t [objectNum];
		for (int i=0; i<objectNum; i++) {
			pthread_rwlock_init(&rwlock[i], NULL);
		}
	}
	~ObjectIDRWLock() {
		for (int i=0; i<objectNum; i++) {
			pthread_rwlock_destroy(&rwlock[i]);
		}
		delete [] rwlock;
	}
	void rdlockObject(int objid);
	void wrlockObject(int objid);
	void unlockObject(int objid);
};

class ObjectRWLock {
private:
	# define RWLOCK_SLOT_NUM 4096
	pthread_rwlock_t rwlocks[RWLOCK_SLOT_NUM];

	void _testSetRead(int slot);
	void _testSetWrite(int slot);
	void _release(int slot);
public:
	ObjectRWLock() {
		for (int i=0; i<RWLOCK_SLOT_NUM; i++) {
			pthread_rwlock_init(&rwlocks[i], NULL);
		}
	}

	void rdlockObject(KStr key);
	void wrlockObject(KStr key);
	void unlockObject(KStr key);
};

class BlockMutex {
private:
	# define LOCK_SLOT_NUM 277
	pthread_spinlock_t spin[LOCK_SLOT_NUM];

	void _testSet(int slot);
	void _release(int slot);
public:
	BlockMutex() {
		for (int i=0; i<LOCK_SLOT_NUM; i++)
			pthread_spin_init(&spin[i], PTHREAD_PROCESS_PRIVATE);
	}
	~BlockMutex() {
		for (int i=0; i<LOCK_SLOT_NUM; i++)
			pthread_spin_destroy(&spin[i]);
	}
	void lockBlock(NVMP nvmp);
	void unlockBlock(NVMP nvmp);
};

#endif /* LOCK_H_ */

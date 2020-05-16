/*
 * lock.cpp
 *
 *  Created on: Jan 26, 2018
 *      Author: hhx008
 */

# include "lock.h"
# include "util.h"
# include <unistd.h>
# include <string>
# include "assert.h"


void ObjectIDRWLock::rdlockObject(int objid) {
	pthread_rwlock_rdlock(&rwlock[objid]);
}

void ObjectIDRWLock::wrlockObject(int objid) {
	pthread_rwlock_wrlock(&rwlock[objid]);
}

void ObjectIDRWLock::unlockObject(int objid) {
	pthread_rwlock_unlock(&rwlock[objid]);
}

void ObjectRWLock::_testSetRead(int slot) {
	pthread_rwlock_rdlock(&rwlocks[slot]);
}

void ObjectRWLock::_testSetWrite(int slot) {
	pthread_rwlock_wrlock(&rwlocks[slot]);
}

void ObjectRWLock::_release(int slot) {
	pthread_rwlock_unlock(&rwlocks[slot]);
}

void ObjectRWLock::rdlockObject(KStr key) {
	_testSetRead(DJBHash(key) % RWLOCK_SLOT_NUM);
}

void ObjectRWLock::wrlockObject(KStr key) {
	_testSetWrite(DJBHash(key) % RWLOCK_SLOT_NUM);
}

void ObjectRWLock::unlockObject(KStr key) {
	_release(DJBHash(key) % RWLOCK_SLOT_NUM);
}

void BlockMutex::_testSet(int slot) {
	pthread_spin_lock(&spin[slot]);
}

void BlockMutex::_release(int slot) {
	pthread_spin_unlock(&spin[slot]);
}

void BlockMutex::lockBlock(NVMP nvmp) {
	_testSet(nvmp % LOCK_SLOT_NUM);
}

void BlockMutex::unlockBlock(NVMP nvmp) {
	_release(nvmp % LOCK_SLOT_NUM);
}

/*
 * Implementation of OCCHash
 *
 *  Created on: Oct 26, 2017
 *      Author: hhx008
 */

# include "nvm.h"
# include "util.h"
# include "OCCHash.h"
# include "global.h"

/* ensure lock protection before call, it's protected
 * by optimistic lock in Insert(), Lookup() and mutex in Delete()
 * assign result bucket and slot index to b & slot_idx
 */
object* OCCHash::locate(KStr key, bucket* & b, int & slot_idx) {
	Hash hash = FNV1Ahash(key) % bucketSize;
	Tag tag = TAGhash(key);
	KStr str;
	itos(tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	object* fobjp = NULL;
	b = &table[hash];
	// check first bucket
	for (int i=0; i<SLOT_NUM; i++) {
		if (!b->slots[i].commit)
			continue;
		object* objp = TO_OBJP(b->slots[i].objp);
		if (objp == NULL)
			continue;
		if (b->slots[i].tag == tag) {
			if (strcmp(objp->key, key) == 0) {
				slot_idx = i;
				fobjp = objp;
				break;
			}
		}
	}
	// check the other bucket
	if (fobjp == NULL) {
		b = &table[hash2];
		for (int i=0; i<SLOT_NUM; i++) {
			if (!b->slots[i].commit)
				continue;
			object* objp = TO_OBJP(b->slots[i].objp);
			if (objp == NULL)
				continue;
			if (b->slots[i].tag == tag) {
				if (strcmp(objp->key, key) == 0) {
					slot_idx = i;
					fobjp = objp;
					break;
				}
			}
		}
	}

	return fobjp;
}

/* ensure lock protection before call, it's protected
 * by optimistic lock & mutex in Insert() and Resize()
 */
void OCCHash::kickout(int pathlen, slot* path[MAXIMUM_DISPLACE], Tag tag, object* objp) {
	// reverse kick-out
	for (int i=1; i<pathlen; i++) {
		slot* sprev = path[i-1];
		slot* scurr = path[i];
		sprev->commit = false;
		persist((void *)sprev, sizeof(slot));
		sprev->tag = scurr->tag;
		sprev->objp = scurr->objp;
		persist((void *)sprev, sizeof(slot));
		sprev->commit = true;
		persist((void *)sprev, sizeof(slot));
	}

	path[pathlen-1]->commit = false;
	persist((void *)path[pathlen-1], sizeof(slot));
	path[pathlen-1]->tag = tag;
	path[pathlen-1]->objp = TO_NVMP(objp);
	persist((void *)path[pathlen-1], sizeof(slot));
	path[pathlen-1]->commit = true;
	persist((void *)path[pathlen-1], sizeof(slot));
}

int OCCHash::pathSearch(Hash hash, Hash hash2, slot* path[MAXIMUM_DISPLACE]) {
	int pathidx, tmplen, pathlen = MAXIMUM_DISPLACE + 1;
	slot* pathset[2*SLOT_NUM][MAXIMUM_DISPLACE];

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

int OCCHash::pathSearchHelper(Hash hash, int slot_idx, int depth, slot* path[MAXIMUM_DISPLACE]) {
	if (depth > MAXIMUM_DISPLACE)
		return MAXIMUM_DISPLACE + 1;

	slot* s = &table[hash].slots[slot_idx];
	KStr str;
	itos(s->tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);
/*
	bucket* b = &table[hash2];
	for (int i=0; i<SLOT_NUM; i++) {
		if (!b->slots[i].commit) { // b->slots[i].objp == NULL
			path[0] = &b->slots[i];
			path[1] = s;
			return 2;
		}
	}
*/
	bucket* b = &table[hash2];
	slot* ts;
	for (int i=0; i<SLOT_NUM; i++) {
		ts = &b->slots[i];
		if (!ts->commit) {
			path[0] = ts;
			path[1] = s;
			return 2;
		} else if (ts->tag == s->tag) {
			if (strcmp(TO_OBJP(ts->objp)->key, TO_OBJP(s->objp)->key) == 0) {
				// there exists duplicated object (caused by crash/shutdown during kickout)
				// so just delete one of them
				path[0] = s;
				return 1;
			}
		}
	}

	// to kick random one (random by % SLOT_NUM) out
	int len = pathSearchHelper(hash2, s->tag % SLOT_NUM, depth+1, path);
	if (len < MAXIMUM_DISPLACE) {
		path[len] = s;
		return len + 1;
	} else return MAXIMUM_DISPLACE + 1;
}

int OCCHash::Size() {
	int s = size.load();
	return s;
}

bool OCCHash::Update(KStr key, object* objp) {
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	Tag tag = TAGhash(key);
	object* tobjp = NULL;
	bucket* b;
	int vc, newvc, slot_idx;

	do {
		vc = counter[hash].load();
		if (vc % 2) {
			// someone's modifying it, set newvc != vc
			newvc = vc + 1;
			continue;
		}

		tobjp = locate(key, b, slot_idx);
		newvc = counter[hash].load();
	} while (newvc != vc);

	if (tobjp == NULL)
		return false;

	pthread_mutex_lock(&writelock);
	counter[hash]++;
	// b->slots[slot_idx].tag = tag;
	b->slots[slot_idx].objp = TO_NVMP(objp);
	persist((void *)(b->slots + slot_idx), sizeof(slot));
	counter[hash]++;
	pthread_mutex_unlock(&writelock);

	return true;
}

// TODO, how many cuckoo paths is best?
bool OCCHash::Insert(KStr key, object* objp) {
	if (Lookup(key) != NULL)
		return true;

	pthread_mutex_lock(&writelock);
	Hash counterhash = APHash(key) % VERSION_COUNTER_SIZE;
	Hash hash = FNV1Ahash(key) % bucketSize;
	Tag tag = TAGhash(key);
	KStr str;
	itos(tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	int slot_idx = -1;
	bucket* b = &table[hash];
	for (int i=0; i<SLOT_NUM; i++)
		if (!b->slots[i].commit/*b->slots[i].objp == NULL*/) {
			slot_idx = i;
			break;
		}
	if (slot_idx < 0) {
		b = &table[hash2];
		for (int i=0; i<SLOT_NUM; i++)
			if (!b->slots[i].commit/*b->slots[i].objp == NULL*/) {
				slot_idx = i;
				break;
			}
	}
	if (slot_idx >= 0) {
		counter[counterhash]++;
		b->slots[slot_idx].tag = tag;
		b->slots[slot_idx].objp = TO_NVMP(objp);
		persist((void *)(b->slots + slot_idx), sizeof(slot));
		b->slots[slot_idx].commit = true;
		persist((void *)(b->slots + slot_idx), sizeof(slot));
		size++;
		counter[counterhash]++;

		pthread_mutex_unlock(&writelock);
		return true;
	}

	slot* targetpath[MAXIMUM_DISPLACE];
	int pathlen = pathSearch(hash, hash2, targetpath);
	if (pathlen < MAXIMUM_DISPLACE + 1) {
		counter[counterhash]++;
		kickout(pathlen, targetpath, tag, objp);
		size++;
		counter[counterhash]++;
	} else {
		pthread_mutex_unlock(&writelock);
		return false;
	}

	pthread_mutex_unlock(&writelock);
	return true;
}

object* OCCHash::Lookup(KStr key) {
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	object* objp = NULL;
	bucket* b;
	int vc, newvc, slot_idx;

	do {
		vc = counter[hash].load();
		if (vc % 2) {
			// someone's modifying it, set newvc != vc
			newvc = vc + 1;
			continue;
		}

		objp = locate(key, b, slot_idx);
		newvc = counter[hash].load();
	} while (newvc != vc);

	return objp;
}

object* OCCHash::Delete(KStr key) {
	pthread_mutex_lock(&writelock);
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	bucket* b;
	int slot_idx;

	object* objp = locate(key, b, slot_idx);
	if (objp != NULL) {
		counter[hash]++;
		size--;
		b->slots[slot_idx].commit = false;
		b->slots[slot_idx].objp = NULL;
		persist((void *)(b->slots + slot_idx), sizeof(slot));
		counter[hash]++;
	}

	pthread_mutex_unlock(&writelock);
	return objp;
}


/* ensure lock protection before call, it's protected
 * by optimistic lock in Insert(), Lookup() and mutex in Delete()
 * assign result bucket and slot index to b & slot_idx
 */
object* OCCHashCache::locate(KStr key, cacheBucket* & b, int & slot_idx) {
	Hash hash = FNV1Ahash(key) % bucketSize;
	Tag tag = TAGhash(key);
	KStr str;
	itos(tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	object* fobjp = NULL;
	b = &table[hash];
	// check first bucket
	for (int i=0; i<SLOT_NUM; i++) {
		object* objp = b->slots[i].objp;
		if (objp == NULL)
			continue;
		if (b->slots[i].tag == tag) {
			if (strcmp(objp->key, key) == 0) {
				slot_idx = i;
				fobjp = objp;
				break;
			}
		}
	}
	// check the other bucket
	if (fobjp == NULL) {
		b = &table[hash2];
		for (int i=0; i<SLOT_NUM; i++) {
			object* objp = b->slots[i].objp;
			if (objp == NULL)
				continue;
			if (b->slots[i].tag == tag) {
				if (strcmp(objp->key, key) == 0) {
					slot_idx = i;
					fobjp = objp;
					break;
				}
			}
		}
	}

	return fobjp;
}

/* ensure lock protection before call, it's protected
 * by optimistic lock & mutex in Insert() and Resize()
 */
// TODO, consistency of kickout
void OCCHashCache::kickout(int pathlen, cacheSlot* path[MAXIMUM_DISPLACE], Tag tag, object* objp) {
	// reverse kick-out
	for (int i=1; i<pathlen; i++) {
		cacheSlot* sprev = path[i-1];
		cacheSlot* scurr = path[i];
		sprev->tag = scurr->tag;
		sprev->objp = scurr->objp;
		// TODO, do we need persist() here?
	}

	path[pathlen-1]->tag = tag;
	path[pathlen-1]->objp = objp;
}

int OCCHashCache::pathSearch(Hash hash, Hash hash2, cacheSlot* path[MAXIMUM_DISPLACE]) {
	int pathidx, tmplen, pathlen = MAXIMUM_DISPLACE + 1;
	cacheSlot* pathset[2*SLOT_NUM][MAXIMUM_DISPLACE];

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

int OCCHashCache::pathSearchHelper(Hash hash, int slot_idx, int depth, cacheSlot* path[MAXIMUM_DISPLACE]) {
	if (depth > MAXIMUM_DISPLACE)
		return MAXIMUM_DISPLACE + 1;

	cacheSlot* s = &table[hash].slots[slot_idx];
	KStr str;
	itos(s->tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	cacheBucket* b = &table[hash2];
	for (int i=0; i<SLOT_NUM; i++) {
		if (b->slots[i].objp == NULL) {
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

int OCCHashCache::Size() {
	int s = size.load();
	return s;
}

bool OCCHashCache::Update(KStr key, object* objp) {
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	Tag tag = TAGhash(key);
	object* tobjp = NULL;
	cacheBucket* b;
	int vc, newvc, slot_idx;

	do {
		vc = counter[hash].load();
		if (vc % 2) {
			// someone's modifying it, set newvc != vc
			newvc = vc + 1;
			continue;
		}

		tobjp = locate(key, b, slot_idx);
		newvc = counter[hash].load();
	} while (newvc != vc);

	if (tobjp == NULL)
		return false;

	pthread_mutex_lock(&writelock);
	counter[hash]++;
	b->slots[slot_idx].tag = tag;
	b->slots[slot_idx].objp = objp;
	counter[hash]++;
	pthread_mutex_unlock(&writelock);

	return true;
}

// TODO, how many cuckoo paths is best?
bool OCCHashCache::Insert(KStr key, object* objp) {
	if (Lookup(key) != NULL)
		return true;

	pthread_mutex_lock(&writelock);
	Hash counterhash = APHash(key) % VERSION_COUNTER_SIZE;
	Hash hash = FNV1Ahash(key) % bucketSize;
	Tag tag = TAGhash(key);
	KStr str;
	itos(tag, str);
	Hash hash2 = hash ^ (FNV1Ahash(str) % bucketSize);

	int slot_idx = -1;
	cacheBucket* b = &table[hash];
	for (int i=0; i<SLOT_NUM; i++) {
		if (b->slots[i].objp == NULL) {
			slot_idx = i;
			break;
		}
	}
	if (slot_idx < 0) {
		b = &table[hash2];
		for (int i=0; i<SLOT_NUM; i++) {
			if (b->slots[i].objp == NULL) {
				slot_idx = i;
				break;
			}
		}
	}
	if (slot_idx >= 0) {
		counter[counterhash]++;
		b->slots[slot_idx].tag = tag;
		b->slots[slot_idx].objp = objp;
		size++;
		counter[counterhash]++;

		pthread_mutex_unlock(&writelock);
		return true;
	}

	cacheSlot* targetpath[MAXIMUM_DISPLACE];
	int pathlen = pathSearch(hash, hash2, targetpath);
	if (pathlen < MAXIMUM_DISPLACE + 1) {
		counter[counterhash]++;
		kickout(pathlen, targetpath, tag, objp);
		size++;
		counter[counterhash]++;
	} else {
		pthread_mutex_unlock(&writelock);
		return false;
	}

	pthread_mutex_unlock(&writelock);
	return true;
}

object* OCCHashCache::Lookup(KStr key) {
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	object* objp = NULL;
	cacheBucket* b;
	int vc, newvc, slot_idx;

	do {
		vc = counter[hash].load();
		if (vc % 2) {
			// someone's modifying it, set newvc != vc
			newvc = vc + 1;
			continue;
		}

		objp = locate(key, b, slot_idx);
		newvc = counter[hash].load();
	} while (newvc != vc);

	return objp;
}

// TODO, finer lock?
object* OCCHashCache::Delete(KStr key) {
	pthread_mutex_lock(&writelock);
	Hash hash = APHash(key) % VERSION_COUNTER_SIZE;
	cacheBucket* b;
	int slot_idx;

	object* objp = locate(key, b, slot_idx);
	if (objp != NULL) {
		counter[hash]++;
		size--;
		b->slots[slot_idx].objp = NULL;
		counter[hash]++;
	}

	pthread_mutex_unlock(&writelock);
	return objp;
}

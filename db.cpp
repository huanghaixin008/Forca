/*
 * db.cpp
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

# include "db.h"
# include <stdio.h>
# include <errno.h>
# include <cstring>
# include <time.h>
# include <sys/time.h>
# include <utility>
# include <iostream>
# include <set>
# include <pthread.h>
# include "object.h"
# include "gc.h"

void * DB::doRecoverRoutine(void * args) {
	RecoverParam * rp = (RecoverParam *) args;
	DB * db = rp->db;
	int start = rp->start;
	int end = rp->end;
	delete rp;

	object * objpool = nvm->getObjPool();
	for (int i=start; i<end; i++) {
		if (objpool[i].used) {
			// std::cout << "recovering object " << TO_NVMP(objpool+i) << std::endl;
			assert(db->hash.Insert(objpool[i].key, objpool + i));
			nvm->markObjUsed(&objpool[i]);
			// scan meta & data belonging to it
			metablock * curr = TO_METAP(objpool[i].logtail);
			metablock * next;
			int loglen = 0;
			while (curr) {
				loglen++;
				next = TO_METAP(curr->peer);
				nvm->markMetaUsed(curr);

				datablock * datacurr = TO_DATAP(curr->data);
				datablock * datanext;
				while (datacurr) {
					datanext = TO_DATAP(datacurr->next);
					nvm->markDataUsed(datacurr);
					nvm->markRegionUsed(TO_REGIONP(datacurr->region), size2power(datacurr->size));
					datacurr = datanext;
				}
				curr = next;
			}
			db->gcinfo.objectloglen[i] = loglen;
		}
	}
	std::cout << "db->recover() thread " << pthread_self() << " ends" << std::endl;
}

void DB::recover() {
	std::cout << "db->recover() starts" << std::endl;	

	pthread_t tid;
	std::vector<pthread_t> pthv;
	unsigned long long rtime = 0;
	timeval start, end;
	gettimeofday(&start, NULL);
	int ration = OBJECT_NUM / RECOVER_THREAD_NUM;
	for (int i=0; i<OBJECT_NUM; i+=ration) {
		RecoverParam * rp = new RecoverParam(this, i, i + ration);
		pthread_create(&tid, NULL, doRecoverRoutine, (void *)rp);
		pthv.push_back(tid);
	}
	for (auto & p : pthv)
		pthread_join(p, NULL);
	nvm->initFreelist();
	gettimeofday(&end, NULL);
	
	std::cout << "db->recover() ends, recover time: " << (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec) << std::endl;
}

void DB::init() {
	std::cout << "db->init() starts" << std::endl;
	nvm->init();
	hash.init();

	metareadtime = new std::atomic<time_t> [METABLOCK_NUM];
	for (int i=0; i<METABLOCK_NUM; i++)
		metareadtime[i] = 0;
	recover();
	std::cout << "db->init() ends" << std::endl;
	return;

}

// a freshly created object has undefined value!
bool DB::create(KStr key, int len) {
	if (len > REGION_MAX_SIZE)
		return false;
	objectlock.wrlockObject(key);
	if (hash.Lookup(key) != NULL) {
		// the obj to create exists
		objectlock.unlockObject(key);
		return false;
	}

	object * objp = nvm->allocateObject();
	assert(objp != NULL);
	strcpy(objp->key, key);
	objp->size = len;
	objp->logtail = NULL;
	gcinfo.objectopcount[OBJP_TO_OBJIDX(objp)] = 0;
	gcinfo.objectloglen[OBJP_TO_OBJIDX(objp)] = 0;
	persist((void *)objp, sizeof(object));
	// commit
	objp->used = true;
	persist((void *)objp, sizeof(object));
	assert(hash.Insert(key, objp));

	objectlock.unlockObject(key);
	return true;
}

bool DB::exist(KStr key) {
	bool flag = false;
	objectlock.rdlockObject(key);
	object * objp = hash.Lookup(key);
	if (objp != NULL)
		flag = true;

	objectlock.unlockObject(key);
	return flag;
}

bool DB::remove(KStr key) {
	objectlock.wrlockObject(key);
	object * objp = hash.Lookup(key);
	if (objp == NULL) {
		// the obj to remove does not exist
		objectlock.unlockObject(key);
		return false;
	}

	objp->used = false;
	persist((void *)objp, sizeof(object));
	assert(hash.Delete(key));
	// remove thread to remove meta & data
	doRemove(TO_METAP(objp->logtail));
	nvm->freeObject(objp);

	objectlock.unlockObject(key);
	return true;
}

// grab proper lock before enter such function!
void DB::doRemove(metablock * logtail) {
	pthread_t removeThread;
	RemoveParam * rp = new RemoveParam(this, logtail);
	pthread_create(&removeThread, NULL, doRemoveRoutine, (void *)rp);
	pthread_detach(removeThread);
	// sleep(1);
}

void * DB::doRemoveRoutine(void * args) {
	RemoveParam * rp = (RemoveParam *) args;
	DB * db = ((RemoveParam*)args)->db;
	metablock * logtail = ((RemoveParam*)args)->meta;
	metablock * curr, * next;

	delete rp;

	bool flag = false;
	while (true) {
		curr = logtail;
		while (!flag && curr) {
			next = TO_METAP(curr->peer);
			ObjectOps::CHECK_RET cr = ObjectOps::validCheck(db, curr);
			switch (cr.vs) {
			case OBJ_INPROGRESS:
				flag = true;
				break;
			case OBJ_VALID:
				// the meta is still being read
				if (db->metareadtime[METAP_TO_METAIDX(curr)]
						+ TIMEOUT_TIME >= time(0)) {
					flag = true;
					break;
				}
			case OBJ_INVALID:
			default:
				curr = next;
			}
		}
		if (!flag) {
			curr = logtail;
			while (curr) {
				next = TO_METAP(curr->peer);
				datablock * datacurr = TO_DATAP(curr->data);
				datablock * datanext;
				while (datacurr) {
					datanext = TO_DATAP(datacurr->next);
					nvm->freeDataBlock(datacurr);
					nvm->freeRegion(TO_REGIONP(datacurr->region));
					datacurr = datanext;
				}
				nvm->freeMetaBlock(curr);
				curr = next;
			}
			break;
		}
	}
	return NULL;
}

int DB::extend(KStr key, int len) {
	if (len <= 0)
		return -1;
	objectlock.wrlockObject(key);
	object * objp = hash.Lookup(key);
	if (objp == NULL) {
		// the obj to extend does not exist
		objectlock.unlockObject(key);
		return false;
	}
	if (objp->size + len > REGION_MAX_SIZE) {
		// extend size exceeds maximum size
		objectlock.unlockObject(key);
		return -1;
	}

	int ret = objp->size = objp->size + len;

	objectlock.unlockObject(key);
	return ret;
}

// the garbage data blocks produced by truncating will be handled by GC
int DB::truncate(KStr key, int len) {
	if (len <= 0)
		return -1;
	objectlock.wrlockObject(key);
	object * objp = hash.Lookup(key);
	if (objp == NULL) {
		// the obj to truncate does not exist
		objectlock.unlockObject(key);
		return -1;
	}
	if (objp->size <= len) {
		// truncate size exceeds object size
		objectlock.unlockObject(key);
		return -1;
	}

	int ret = objp->size = objp->size - len;

	objectlock.unlockObject(key);
	return ret;
}

bool DB::pureRead(KStr key, int offset, char buf[], int len) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.rdlockObject(key);
	object * objp = hash.Lookup(key);
	if (!objp) {
		// the object to read does not exist
		objectlock.unlockObject(key);
		return false;
	}

	if (offset + len > objp->size) {
		// try to read out of boundary
		objectlock.unlockObject(key);
		return false;
	}

	metablock * meta = TO_METAP(objp->logtail);
	if (meta == NULL) {
		datablock * data = nvm->allocateDataBlock();
		assert(data != NULL);
		data->next = NULL;
		data->offset = 0;
		data->size = objp->size;
		data->region = TO_NVMP(nvm->allocateRegion(size2power(data->size)));
		persist((void *)data, sizeof(datablock));

		meta = nvm->allocateMetaBlock();
		assert(meta != NULL);
		meta->peer = NULL;
		meta->valid = OBJ_VALID;
		meta->data = TO_NVMP(data);
		persist((void *)meta, sizeof(metablock));
		objp->logtail = TO_NVMP(meta);
		persist((void *)objp, sizeof(object));
	}

	std::vector<OPI> opiv = ObjectOps::fallGetOPI(this, objp, offset, offset + len);
	for (auto & opi : opiv)
		memcpy(buf + opi.offset - offset, opi.region, opi.size);

	objectlock.unlockObject(key);
	return true;
}

bool DB::pureWrite(KStr key, int offset, char buf[], int len) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.wrlockObject(key);

	object * objp = hash.Lookup(key);
	int objidx = OBJP_TO_OBJIDX(objp);
	if (!objp) {
		// the obj to write does not exist
		objectlock.unlockObject(key);
		return false;
	}
	if (offset + len > objp->size) {
		// the obj to write exceeds the obj size
		objectlock.unlockObject(key);
		return false;
	}
	metablock * meta = TO_METAP(objp->logtail);
	if (meta == NULL) {
		datablock * data = nvm->allocateDataBlock();
		assert(data != NULL);
		data->next = NULL;
		data->offset = 0;
		data->size = objp->size;
		char * region = nvm->allocateRegion(size2power(data->size));
		data->region = TO_NVMP(region);
		persist((void *)data, sizeof(datablock));

		meta = nvm->allocateMetaBlock();
		assert(meta != NULL);
		meta->peer = NULL;
		meta->valid = OBJ_VALID;
		meta->data = TO_NVMP(data);
		persist((void *)meta, sizeof(metablock));
		objp->logtail = TO_NVMP(meta);
		persist((void *)objp, sizeof(object));
	}

	std::vector<OPI> opiv = ObjectOps::fallGetOPI(this, objp, offset, offset + len);
	for (auto & opi : opiv) {
		memcpy(opi.region, buf + opi.offset - offset, opi.size);
		persist((void *)opi.region, opi.size);
	}

	objectlock.unlockObject(key);
	return true;
}

bool DB::read(KStr key, int offset, char buf[], int len) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.rdlockObject(key);
	object * objp = hash.Lookup(key);
	if (!objp) {
		// the object to read does not exist
		objectlock.unlockObject(key);
		return false;
	}

	if (offset + len > objp->size) {
		// try to read out of boundary
		objectlock.unlockObject(key);
		return false;
	}

	// std::cout << "during read, logtail: " << objp->logtail << std::endl;
	std::vector<OPI> opiv = ObjectOps::fallGetOPI(this, objp, offset, offset + len);
	if (opiv.size() == 0) {
		// nothing to read
		objectlock.unlockObject(key);
		return true;
	}
	// for DEBUG
	// int count = 0;
	// std::set<std::pair<int, int>> s;
	for (auto & opi : opiv) {
		// metareadtime[METAP_TO_METAIDX(opi.meta)] = time(0);
		// std::cout << "during read, OPI: " << TO_NVMP(opi.meta) << " " << TO_NVMP(opi.region) << " " << opi.offset << " " << opi.size << std::endl;
		memcpy(buf + opi.offset - offset, opi.region, opi.size);
		// s.insert(std::make_pair(opi.goffset, opi.goffset + opi.len));
		// count += opi.len;
	}
	/*
	assert(count <= len);
	std::pair<int, int> prev = std::make_pair(0, offset);
	for (auto it = s.begin(); it != s.end(); it++) {
		// std::cout << prev.first << " " << prev.second << " " << it->first << " " << it->second << std::endl;
		assert(it->first >= prev.second);
		prev = *it;
	} */

	objectlock.unlockObject(key);
	return true;
}

bool DB::write(KStr key, int offset, char buf[], int len, unsigned int crc32) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.wrlockObject(key);

	object * objp = hash.Lookup(key);
	int objidx = OBJP_TO_OBJIDX(objp);
	if (!objp) {
		// the obj to write does not exist
		objectlock.unlockObject(key);
		return false;
	}
	if (offset + len > objp->size) {
		// the obj to write exceeds the obj size
		objectlock.unlockObject(key);
		return false;
	}

	metablock * newmeta = nvm->allocateMetaBlock();
	assert(newmeta != NULL);
	newmeta->valid = OBJ_INPROGRESS;
	newmeta->timestamp = time(0);
	newmeta->crc32 = crc32;
	char * region = nvm->allocateRegion(size2power(len));
	assert(region != NULL);
	memcpy(region, buf, len);
	persist((void *)region, len);

	datablock * newdata = nvm->allocateDataBlock();
	assert(newdata != NULL);
	newdata->offset = offset;
	newdata->size = len;
	newdata->next = NULL;
	newdata->region = TO_NVMP(region);
	persist((void *)newdata, sizeof(datablock));
	newmeta->data = TO_NVMP(newdata);
	newmeta->peer = objp->logtail;
	persist((void *)newmeta, sizeof(metablock));
	// commit obj write
	objp->logtail = TO_NVMP(newmeta);
	persist((void *)objp, sizeof(object));

	gcinfo.objectopcount[objidx]++;
	gcinfo.objectloglen[objidx]++;
	if (gcinfo.objectopcount[objidx] > MAX_UNGCED_COUNT) {
		GCOps::doGC(this, objp);
		gcinfo.objectopcount[objidx] = 0;
	}

	objectlock.unlockObject(key);
	return true;
}

int count = 0;
timeval start, end, startt, endt;
unsigned long long totaltime = 0;
unsigned long long allocatetime = 0;
unsigned long long GCtime = 0;
unsigned long long SFtime = 0;

bool DB::lazyRead(KStr key, int offset, int len, std::vector<OPI> & opiv) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.rdlockObject(key);
	object * objp = hash.Lookup(key);
	if (!objp) {
		// the object to read does not exist
		objectlock.unlockObject(key);
		return false;
	}

	if (offset + len > objp->size) {
		// try to read out of boundary
		objectlock.unlockObject(key);
		return false;
	}

	gettimeofday(&start, NULL);	
	opiv = ObjectOps::fallGetOPI(this, objp, offset, offset + len);
	gettimeofday(&end, NULL);
	SFtime += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec); 	
	
	objectlock.unlockObject(key);
	count++;
	if (count >= 1000000) {
		std::cout << "SF time: " << SFtime << "totaltime: " << totaltime << ", allocate time: " << allocatetime << ", GCtime: " << GCtime << std::endl;
		count = SFtime = totaltime = allocatetime = GCtime = 0;
	}
	return true;
}


bool DB::lazyWrite(KStr key, int offset, int len, unsigned int crc32, std::vector<OPI> & opiv) {
	gettimeofday(&startt, NULL);
	if (offset < 0 || len <= 0)
		return false;
	objectlock.wrlockObject(key);
	object * objp = hash.Lookup(key);
	int objidx = OBJP_TO_OBJIDX(objp);
	if (!objp) {
		// the obj to write does not exist
		objectlock.unlockObject(key);
		return false;
	}
	if (offset + len > objp->size) {
		// the obj to write exceeds the obj size
		objectlock.unlockObject(key);
		return false;
	}

	// gettimeofday(&start, NULL);
	metablock * newmeta = nvm->allocateMetaBlock();
	// gettimeofday(&end, NULL);
	// allocatetime += (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
	assert(newmeta != NULL);
	newmeta->valid = OBJ_INPROGRESS;
	newmeta->timestamp = time(0);
	newmeta->crc32 = crc32;

	datablock * newdata = nvm->allocateDataBlock();
	char * region = nvm->allocateRegion(size2power(len));
	// gettimeofday(&end, NULL);
	// allocatetime += (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
	assert(newdata != NULL);
	assert(region != NULL);
	newdata->offset = offset;
	newdata->size = len;
	newdata->next = NULL;
	newdata->region = TO_NVMP(region);
	persist((void *)newdata, sizeof(datablock));
	newmeta->data = TO_NVMP(newdata);
	newmeta->peer = objp->logtail;
	persist((void *)newmeta, sizeof(metablock));
	// commit obj write
	objp->logtail = TO_NVMP(newmeta);
	persist((void *)objp, sizeof(object));
	opiv.push_back(OPI(newmeta, newdata, region, offset, len));
	gcinfo.objectopcount[objidx]++;
	gcinfo.objectloglen[objidx]++;
	if (gcinfo.objectopcount[objidx] > MAX_UNGCED_COUNT) {
		gettimeofday(&start, NULL);
		GCOps::doGC(this, objp);
		gettimeofday(&end, NULL);
		GCtime += (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
		gcinfo.objectopcount[objidx] = 0;
	}

	objectlock.unlockObject(key);
	gettimeofday(&endt, NULL);
	totaltime += (endt.tv_sec - startt.tv_sec)*1000000 + (endt.tv_usec - startt.tv_usec);
	count++;
	if (count >= 1000000) {
		std::cout << "SF time: " << SFtime << "totaltime: " << totaltime << ", allocate time: " << allocatetime << ", GCtime: " << GCtime << std::endl;
		count = SFtime = totaltime = allocatetime = GCtime = 0;
	}
	return true;
}

bool DB::inplaceLazyRead(KStr key, int offset, int len, std::vector<OPI> & opiv) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.rdlockObject(key);
	object * objp = hash.Lookup(key);
	if (!objp) {
		// the object to read does not exist
		objectlock.unlockObject(key);
		return false;
	}

	if (offset + len > objp->size) {
		// try to read out of boundary
		objectlock.unlockObject(key);
		return false;
	}

	metablock * meta = TO_METAP(objp->logtail);
	if (meta == NULL) {
		datablock * data = nvm->allocateDataBlock();
		assert(data != NULL);
		data->next = NULL;
		data->offset = 0;
		data->size = objp->size;
		data->region = TO_NVMP(nvm->allocateRegion(size2power(data->size)));
		persist((void *)data, sizeof(datablock));

		meta = nvm->allocateMetaBlock();
		assert(meta != NULL);
		meta->peer = NULL;
		meta->valid = OBJ_VALID;
		meta->data = TO_NVMP(data);
		persist((void *)meta, sizeof(metablock));
		objp->logtail = TO_NVMP(meta);
		persist((void *)objp, sizeof(object));
	}

	opiv = ObjectOps::fallGetOPI(this, objp, offset, offset + len);

	// do not unlock here! unlocked by finishRead()
	// objectlock.rdunlockObject(key);
	return true;
}

bool DB::inplaceLazyWrite(KStr key, int offset, int len, std::vector<OPI> & opiv) {
	if (offset < 0 || len <= 0)
		return false;
	objectlock.wrlockObject(key);

	object * objp = hash.Lookup(key);
	int objidx = OBJP_TO_OBJIDX(objp);
	if (!objp) {
		// the obj to write does not exist
		objectlock.unlockObject(key);
		return false;
	}
	if (offset + len > objp->size) {
		// the obj to write exceeds the obj size
		objectlock.unlockObject(key);
		return false;
	}

	metablock * meta = TO_METAP(objp->logtail);
	if (meta == NULL) {
		datablock * data = nvm->allocateDataBlock();
		assert(data != NULL);
		data->next = NULL;
		data->offset = 0;
		data->size = objp->size;
		data->region = TO_NVMP(nvm->allocateRegion(size2power(data->size)));
		persist((void *)data, sizeof(datablock));

		meta = nvm->allocateMetaBlock();
		assert(meta != NULL);
		meta->peer = NULL;
		meta->valid = OBJ_VALID;
		meta->data = TO_NVMP(data);
		persist((void *)meta, sizeof(metablock));
		objp->logtail = TO_NVMP(meta);
		persist((void *)objp, sizeof(object));
	}

	opiv = ObjectOps::fallGetOPI(this, objp, offset, offset + len);

	// do not unlock here! unlocked by finishWrite()
	// objectlock.wrunlockObject(key);
	return true;
}

void DB::finishRead(KStr key) {
	objectlock.unlockObject(key);
};

void DB::finishWrite(KStr key) {
	object * objp = hash.Lookup(key);
	assert(objp != NULL);
	metablock * meta = TO_METAP(objp->logtail);
	// TODO, finer granularity persist() here
	datablock * data = TO_DATAP(meta->data);
	persist((void*)TO_REGIONP(data->region), data->size);
	objectlock.unlockObject(key);
};

void DB::printObjectLog(KStr key) {
	objectlock.rdlockObject(key);
	object * objp = hash.Lookup(key);
	if (!objp) {
		// the obj to print does not exist
		objectlock.unlockObject(key);
		return;
	}

	std::cout << "<object> " << key << ", " << TO_NVMP(objp) << std::endl;
	metablock * curr = TO_METAP(objp->logtail);
	metablock * next;
	while (curr) {
		next = TO_METAP(curr->peer);
		ObjectOps::CHECK_RET cr = ObjectOps::validCheck(this, curr);
		std::cout << "<meta> " << TO_NVMP(curr) << ", valid: " << cr.vs << std::endl;
		datablock * datacurr = TO_DATAP(curr->data);
		datablock * datanext;
		while (datacurr) {
			datanext = TO_DATAP(datacurr->next);
			std::cout << "<region/> " << (void *) TO_REGIONP(datacurr->region) << ", size: " << datacurr->size << std::endl;
			datacurr = datanext;
		}
		std::cout << "</meta>" << std::endl;
		curr = next;
	}
	std::cout << "</object>" << std::endl;

	objectlock.unlockObject(key);
}

// grab proper lock before entering
bool DB::objectLogLegalCheck(object * objp) {
	metablock * curr = TO_METAP(objp->logtail);
	metablock * next;
	while (curr) {
		next = TO_METAP(curr->peer);
		if (curr == next)
			return false;
		curr = next;
	}

	return true;
}

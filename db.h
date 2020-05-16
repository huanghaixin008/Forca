/*
 * db.h
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

#ifndef DB_H_
#define DB_H_

# include <atomic>
# include <string>
# include <unordered_map>
# include <vector>
# include "assert.h"
# include "global.h"
# include "util.h"
# include "object.h"
# include "nvm.h"
# include "OCCHash.h"
# include "lock.h"
# include "types.h"
# include "gc.h"

# define RECOVER_THREAD_NUM 16
# define MAX_UNGCED_COUNT 5

class DB {
	struct RecoverParam {
		DB * db;
		int start, end;
		RecoverParam(DB * d, int s, int e) : db(d), start(s), end(e) {};
	};
	struct RemoveParam {
		DB * db;
		metablock * meta;
		RemoveParam(DB * d, metablock * m) : db(d), meta(m) {};
	};
	struct GCParam {
		DB * db;
		object * objp;
		KStr key;
		GCParam(DB * d, object * o, KStr k) : db(d), objp(o) { strcpy(key, k); };
	};
private:
	OCCHashCache hash;

	ObjectRWLock objectlock;
	BlockMutex blocklock;
	std::atomic<time_t> * metareadtime; //[METABLOCK_NUM]
	GCInfo gcinfo;

	static void * doRecoverRoutine(void * args);
	static void * doRemoveRoutine(void * args);
	void recover();

public:
	friend class GCOps;
	friend class ObjectOps;

	~DB() {
		delete [] metareadtime;
	};

	void init();

	bool create(KStr key, int len);
	bool exist(KStr key);
	bool remove(KStr key);
	int extend(KStr key, int len);
	int truncate(KStr key, int len);

	// DO NOT mixed use 4 kinds of read/write!
	// 1. inplace: true, lazy: false (pure msg passing, no consistency mechanism)
	bool pureRead(KStr key, int offset, char buf[], int len);
	bool pureWrite(KStr key, int offset, char buf[], int len);
	// 2. inplace: false, lazy: false
	bool read(KStr key, int offset, char buf[], int len);
	bool write(KStr key, int offset, char buf[], int len, unsigned int crc32);
	// 3. inplace: false, lazy: true (Forca)
	bool lazyRead(KStr key, int offset, int len, std::vector<OPI> & opiv);
	bool lazyWrite(KStr key, int offset, int len, unsigned int crc32, std::vector<OPI> & opiv);
	// 4. inplace: true, lazy: true (Octopus)
	bool inplaceLazyRead(KStr key, int offset, int len, std::vector<OPI> & opiv);
	bool inplaceLazyWrite(KStr key, int offset, int len, std::vector<OPI> & opiv);
	void finishRead(KStr key);
	void finishWrite(KStr key);

	void printObjectLog(KStr key);
	bool objectLogLegalCheck(object * objp);

	void doRemove(metablock * logtail);
	void doGC(object * objp);

	unsigned long long getObjectUsage() { return nvm->getObjectUsage(); };
	unsigned long long getMetaUsage() { return nvm->getMetaUsage(); };
	unsigned long long getDataUsage() { return nvm->getDataUsage(); };
	unsigned long long getRegionUsage() { return nvm->getRegionUsage(); };
};

#endif /* DB_H_ */

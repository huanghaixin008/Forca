/*
 * gc.h
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

#ifndef GC_H_
#define GC_H_

# include <atomic>
# include "pthread.h"
# include "global.h"
# include <cstring>
# include <unordered_set>
# include <unordered_map>

# define MAX_COMPACT_NUM 30
# define GC_SLEEPTIME (5000*1000) // 5s

class DB;

class GCInfo {
public:
	int * objectopcount;
	int * objectloglen;
	GCInfo() {
		objectopcount = new int [OBJECT_NUM];
		objectloglen = new int [OBJECT_NUM];
		memset(objectopcount, 0, sizeof(int) * OBJECT_NUM);
		memset(objectloglen, 0, sizeof(int) * OBJECT_NUM);
	}
	~GCInfo() {
		delete [] objectopcount;
		delete [] objectloglen;
	}
};

class GCOps {
private:
	static void reclaimMeta(metablock * meta);
	static metablock * compressMeta(std::vector<metablock *> & cmprmeta, std::vector<datablock *> & cmprdatatail,
			metablock * next);
	static int collectDeadBlock(std::unordered_set<NVMP> & metaset, std::unordered_set<NVMP> & dataset,
			DB * db, object * objp, metablock * meta);
	static int shrinkObjectLog(DB * db, object * objp);
public:
	static int doGC(DB * db, object * objp);
};

#endif /* GC_H_ */

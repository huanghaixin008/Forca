/*
 * object.h
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

#ifndef OBJECT_H_
#define OBJECT_H_

# include <time.h>
# include <vector>
# include <set>
# include "const.h"
# include "types.h"
# include "lock.h"

# define TIMEOUT_TIME 10 // 10 second
# define OBJECT_LOG_ALLOWEDLEN 30

# define OBJECT_ALLOCPOWER 0
# define META_ALLOCPOWER 0
# define DATA_ALLOCPOWER 0
# define REGION_ALLOCPOWER 5
# define REGION_MAX_SIZE ((1 << REGION_ALLOCPOWER) * REGIONBLOCK_SIZE)

class DB;

enum VALID_STATE {
	OBJ_INPROGRESS = 0,
	OBJ_VALID,
	OBJ_INVALID,
};

struct object {
	bool used;
	bool align;
	int size;
	KStr key;
	NVMP logtail;
};

struct metablock {
	NVMP peer;
	time_t timestamp;
	unsigned int crc32;
	BYTE valid;
	NVMP data;
};

struct datablock {
	int offset;
	int size;
	NVMP region;
	NVMP next;
};

// object position info
struct OPI {
	metablock * meta;
	datablock * data;
	char * region;
	int offset;
	int size;
	OPI(metablock * m, datablock * d, char * r, int o, int s) :
		meta(m), data(d), region(r), offset(o), size(s) {};
};

class ObjectOps {
public:
	struct CHECK_RET {
		VALID_STATE vs;
		bool change;
		CHECK_RET(VALID_STATE v, bool c) : vs(v), change(c) {};
	};
	static int intersectExtent(std::vector<OPI> & opiv, std::set<std::pair<int, int>> & readset,
			metablock * meta, datablock * curr);
	static CHECK_RET validCheck(DB * db, metablock * meta, bool deep = true);
	static bool timeoutCheck(metablock * meta);
	static std::vector<OPI> fallGetOPI(DB * db, object * objp, int start, int end);
	static unsigned long long calCRC32(datablock * data);

};

#endif /* OBJECT_H_ */

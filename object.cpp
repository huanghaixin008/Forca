/*
 * object.cpp
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

# include "object.h"
# include "util.h"
# include <set>
# include <utility>
# include <iostream>
# include "db.h"

// CAUTIOUS, need to grab proper lock before call this function
ObjectOps::CHECK_RET ObjectOps::validCheck(DB * db, metablock * meta, bool deep) {
	db->blocklock.lockBlock(TO_NVMP(meta));
	if (meta->valid == OBJ_VALID) {
		db->blocklock.unlockBlock(TO_NVMP(meta));
		return CHECK_RET(OBJ_VALID, false);
	} else if (meta->valid == OBJ_INVALID) {
		db->blocklock.unlockBlock(TO_NVMP(meta));
		return CHECK_RET(OBJ_INVALID, false);
	}

	if (deep) {
		// note that: a combined meta could only be VALID, so we don't have to consider how to check it
		// i.e., only single meta could be OBJ_INPROGRESS, combined meta are always OBJ_VALID
		datablock * data = TO_DATAP(meta->data);
		assert(data->next == NULL);
		if (calCRC32(data) == meta->crc32) {
			// persist data here before mark it as OBJ_VALID
			persist((void *)TO_REGIONP(data->region), data->size);
			meta->valid = OBJ_VALID;
			persist((void *)meta, sizeof(metablock));
			db->blocklock.unlockBlock(TO_NVMP(meta));
			return CHECK_RET(OBJ_VALID, true);
		} else if (timeoutCheck(meta)) {
			meta->valid = OBJ_INVALID;
			persist((void *)meta, sizeof(metablock));
			db->blocklock.unlockBlock(TO_NVMP(meta));
			return CHECK_RET(OBJ_INVALID, true);
		}
	}

	db->blocklock.unlockBlock(TO_NVMP(meta));
	return CHECK_RET(OBJ_INPROGRESS, false);
}

bool ObjectOps::timeoutCheck(metablock * meta) {
	time_t curr = time(0);
	time_t record = meta->timestamp;
	if (record + TIMEOUT_TIME < curr)
		return true;
	else return false;
}

int ObjectOps::intersectExtent(std::vector<OPI> & opiv, std::set<std::pair<int, int>> & readset,
		metablock * meta, datablock * curr) {
	int glow = curr->offset, ghigh = curr->offset + curr->size;
	int added = 0;
	char * region = TO_REGIONP(curr->region);

	// traverse extent of read
	auto begin = readset.lower_bound(std::make_pair(glow, 0));
	auto end = readset.lower_bound(std::make_pair(ghigh, 0));
	if (begin != readset.begin()) {
		begin--;
		if (begin->second > glow) {
			// CAUTION, filling opi also happens here (as well as reset meta readtime)
			if (ghigh < begin->second) {
				opiv.push_back(OPI(meta, curr, region, glow, ghigh - glow));
				added++;
			}
			else {
				opiv.push_back(OPI(meta, curr, region, glow, begin->second - glow));
				added++;
			}
		}
		begin++;
	}
	for (auto it = begin; it != end; it++) {
		// CAUTION, filling opi also happens here (as well as reset meta readtime)
		if (ghigh < it->second) {
			opiv.push_back(OPI(meta, curr, region + (it->first - glow), it->first, ghigh - it->first));
			added++;
		}
		else {
			opiv.push_back(OPI(meta, curr, region + (it->first - glow), it->first, it->second - it->first));
			added++;
		}
	}

	std::vector<std::pair<int, int>> tmpv;
	begin = readset.lower_bound(std::make_pair(glow, 0));
	end = readset.lower_bound(std::make_pair(ghigh, 0));
	if (begin != readset.begin()) {
		begin--;
		if (begin->second > glow) {
			if (ghigh < begin->second) {
				tmpv.push_back(std::make_pair(begin->first, glow));
				tmpv.push_back(std::make_pair(ghigh, begin->second));
			} else {
				tmpv.push_back(std::make_pair(begin->first, glow));
			}
			begin = readset.erase(begin);
		} else begin++;
	}
	for (auto it = begin; it != end;) {
		if (ghigh < it->second)
			tmpv.push_back(std::make_pair(ghigh, it->second));
		it = readset.erase(it);
	}
	for (auto p : tmpv)
		readset.insert(p);

	return added;
}

std::vector<OPI> ObjectOps::fallGetOPI(DB * db, object * objp, int start, int end) {
	std::vector<OPI> opiv;

	std::set<std::pair<int, int>> readset;
	readset.insert(std::make_pair(start, end));

	metablock * curr = TO_METAP(objp->logtail);
	metablock * next;
	datablock * datacurr;
	bool found = false;
	while (!found && curr) {
		next = TO_METAP(curr->peer);

		CHECK_RET cr = ObjectOps::validCheck(db, curr);
		switch (cr.vs) {
		case OBJ_VALID:
			datacurr = TO_DATAP(curr->data);
			while (datacurr) {
				intersectExtent(opiv, readset, curr, datacurr);
				datacurr = TO_DATAP(datacurr->next);
			}
			if (readset.size() == 0)
				found = true;
		case OBJ_INVALID:
		case OBJ_INPROGRESS:
		default:
			curr = next;
		}
	}

	return opiv;
}

inline unsigned long long ObjectOps::calCRC32(datablock * data) {
	return CRC32(TO_REGIONP(data->region), data->size, 0);
}


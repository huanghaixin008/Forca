/*
 * gc.cpp
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

# include "gc.h"
# include "util.h"
# include <utility>
# include <iostream>
# include <unordered_map>
# include <unordered_set>

# include "db.h"

int total_reclaimed = 0;

void GCOps::reclaimMeta(metablock * meta) {
	datablock * datacurr = TO_DATAP(meta->data);
	datablock * datanext;
	while (datacurr) {
		datanext = TO_DATAP(datacurr->next);
		nvm->freeRegion(TO_REGIONP(datacurr->region));
		nvm->freeDataBlock(datacurr);
		datacurr = datanext;
	}
	nvm->freeMetaBlock(meta);
}

// TODO, debug the function!
metablock * GCOps::compressMeta(std::vector<metablock *> & cmprmeta, std::vector<datablock *> & cmprdatatail,
		metablock * next) {
	int size = cmprmeta.size();
	if (size <= 1)
		return NULL;

	// link cmprdata together, no copy at all!
	metablock * head = cmprmeta[0];
	datablock * tail = cmprdatatail[0];
	// during such procedure, later metas become dead
	for (int i=1; i<size; i++) {
		tail->next = cmprmeta[i]->data;
		persist((void *)tail, sizeof(datablock));
		tail = cmprdatatail[i];
	}
	// commit change
	head->peer = TO_NVMP(next);
	persist((void *)head, sizeof(metablock));
	// free compressed meta
	for (int i=1; i<size; i++)
		nvm->freeMetaBlock(cmprmeta[i]);

	return cmprmeta[0];
}

int GCOps::doGC(DB * db, object * objp) {
	object * tobjp;
	int objidx = OBJP_TO_OBJIDX(objp);
	int collected, shrint = 0;
	std::vector<OPI> opiv;

	// collect GC info here
	opiv = ObjectOps::fallGetOPI(db, objp, 0, objp->size);
	std::unordered_set<NVMP> metaset;
	std::unordered_set<NVMP> dataset;
	for (auto & opi : opiv) {
		metaset.insert(TO_NVMP(opi.meta));
		dataset.insert(TO_NVMP(opi.data));
	}

	collected = collectDeadBlock(metaset, dataset, db, objp, TO_METAP(objp->logtail));
	db->gcinfo.objectloglen[objidx] -= collected;

	// log too long, shrink it!
	// TODO, a better trigger for entering heavy GC?
	if (db->gcinfo.objectloglen[objidx] > OBJECT_LOG_ALLOWEDLEN) {
		shrint = 0;
		db->gcinfo.objectloglen[objidx] -= shrint;
	}

	return collected + shrint;
}

int GCOps::collectDeadBlock(std::unordered_set<NVMP> & metaset, std::unordered_set<NVMP> & dataset,
		DB * db, object * objp, metablock * meta) {
	metablock * prev = NULL;
	metablock * curr = meta;
	metablock * next;
	int reclaimed = 0;

	int checked = 0;
	int inprogress = 0;
	while (curr) {
		checked++;
		next = TO_METAP(curr->peer);
		ObjectOps::CHECK_RET cr = ObjectOps::validCheck(db, curr);
		// the meta is inprogress or it just turns from inprogress
		if (cr.vs == OBJ_INPROGRESS || (cr.change && cr.vs == OBJ_VALID)) {
			inprogress++;
			prev = curr;
			curr = next;
			continue;
		}

		if (cr.vs == OBJ_INVALID) {
			// invalid meta
			if (prev == NULL) {
				objp->logtail = TO_NVMP(next);
				persist((void *)objp, sizeof(object));
			} else {
				prev->peer = TO_NVMP(next);
				persist((void *)prev, sizeof(metablock));
			}
			// persist();
			reclaimMeta(curr);
			reclaimed++;
		} else {
			// if it's being read, ignore it (by the way, a invalid meta could not be being read!)
			if (db->metareadtime[METAP_TO_METAIDX(curr)] + TIMEOUT_TIME > time(0)) {
				std::cout << "???? " << db->metareadtime[METAP_TO_METAIDX(curr)] << std::endl;
				prev = curr;
				curr = next;
				continue;
			}

			if (metaset.count(TO_NVMP(curr)) == 0) {
				// dead meta
				if (prev == NULL) {
					objp->logtail = TO_NVMP(next);
					persist((void *)objp, sizeof(object));
				} else {
					prev->peer = TO_NVMP(next);
					persist((void *)prev, sizeof(metablock));
				}
				// persist();
				reclaimMeta(curr);
				reclaimed++;
			} else {
				// check if any dead data
				datablock * dataprev = NULL;
				datablock * datacurr = TO_DATAP(curr->data);
				datablock * datanext;
				while (datacurr) {
					datanext = TO_DATAP(datacurr->next);
					if (dataset.count(TO_NVMP(datacurr)) == 0) {
						// dead data
						if (dataprev) {
							dataprev->next = TO_NVMP(datanext);
							persist((void *)dataprev, sizeof(datablock));
						} else {
							curr->data = TO_NVMP(datanext);
							persist((void *)curr, sizeof(metablock));
						}
						nvm->freeRegion(TO_REGIONP(datacurr->region));
						nvm->freeDataBlock(datacurr);
					} else dataprev = datacurr;
					datacurr = datanext;
				}
				prev = curr;
			}
		}
		curr = next;
	}

	return reclaimed;
}

// TODO, debug such function!
int GCOps::shrinkObjectLog(DB * db, object * objp) {
	metablock * meta = TO_METAP(objp->logtail), * metaprev = NULL;
	metablock * next, * curr, * prev;
	int reclaimed = 0;

	while (meta) {
		curr = meta;
		prev = metaprev;

		bool out = false;
		bool overlapped = false;
		std::set<std::pair<int, int>> rangeset;
		std::vector<metablock *> cmprmeta;
		std::vector<datablock *> cmprdatatail;
		while (curr) {
			next = TO_METAP(curr->peer);
			ObjectOps::CHECK_RET cr = ObjectOps::validCheck(db, curr, false);
			switch (cr.vs) {
			case OBJ_VALID:
				if (db->metareadtime[METAP_TO_METAIDX(curr)] + TIMEOUT_TIME > time(0)) {
					out = true;
				} else {
					datablock * datacurr = TO_DATAP(meta->data);
					datablock * datatail = NULL;
					while (datacurr) {
						datatail = datacurr;
						std::pair<int, int> range = std::make_pair(datacurr->offset, datacurr->offset + datacurr->size);
						auto it = rangeset.lower_bound(range);
						if (it != rangeset.begin()) {
							it--;
							if (it->second > range.first)
								overlapped = true;
							it++;
						}
						if (it != rangeset.end()) {
							if (it->first < range.second)
								overlapped = true;
						}
						if (overlapped)
							break;
						datacurr = TO_DATAP(datacurr->next);
					}

					if (overlapped) {
						out = true;
					} else {
						cmprmeta.push_back(curr);
						cmprdatatail.push_back(datatail);
					}
				}
				break;
			case OBJ_INVALID:
				// not possible
			case OBJ_INPROGRESS:
			default:
				out = true;
				break;
			}
			if (out)
				break;
			prev = curr;
			curr = next;
		}

		metablock * newmeta = compressMeta(cmprmeta, cmprdatatail, curr);
		if (newmeta)
			reclaimed += cmprmeta.size() - 1;
		if (overlapped) {
			meta = curr;
			metaprev = newmeta == NULL ? prev : newmeta;
		} else {
			meta = next;
			metaprev = curr;
		}
	}

	return reclaimed;
}

/*
 * forcakv-client-pilaf.h
 *
 *  Created on: May 22, 2018
 *      Author: hhx008
 */

#ifndef FORCAKV_CLIENT_PILAF_H_
#define FORCAKV_CLIENT_PILAF_H_

# include "forca-client.h"

# define KV_VALUE_LEN 512
# define NET_MSG_SIZE (4096+512)
# define MAX_SIZE_PER_OP 4096

typedef KStr Key;
typedef char Value[KV_VALUE_LEN];

enum sendMsgType {
	SEND_MSG_SET = 799,
	SEND_MSG_GET,
	SEND_MSG_UNSET,
};

enum recvMsgType {
	RECV_MSG_FAIL,
	RECV_MSG_SUCCESS,
};

class ForcaKVClient : public ForcaClient {
private:
	int opcount = 0;
	int hashfetchcount = 0;
	int valuefetchcount = 0;
public:
	ForcaKVClient() : ForcaClient(NET_MSG_SIZE, MAX_SIZE_PER_OP) {};
	bool set(int id, Key key, Value value, bool direct = false) {
		// std::cout << "===== sending MSG_SET " << id << ", key: " << key << ", value:" << value << std::endl;

		char * bodyptr = RDMAdo(getSendBuf());
		int count = 0;
		memcpy(bodyptr, key, sizeof(Key));
		bodyptr += sizeof(Key);
		count += sizeof(Key);
		*((int *)bodyptr) = KV_VALUE_LEN;
		bodyptr += sizeof(int);
		memcpy(bodyptr, value, sizeof(Value));
		count += sizeof(int);
		RDMAdo(sendMsg(SEND_MSG_SET, NULL, true));
		// std::cout << "===== sending MSG_SET done " << id << std::endl;

		int msgtype;
		RDMAdo(recvMsg(msgtype, NULL, true));
		if (msgtype != RECV_MSG_SUCCESS) {
			// std::cout << "===== MSG_SET fails " << id  << std::endl;
			return false;
		}
		// std::cout << "===== MSG_SET succeeds " << id  << std::endl;
		opcount++;
		if (opcount >= 1000000) {
			opcount = 0;
			std::cout << "hash count: " << hashfetchcount << ", value count: " << valuefetchcount << std::endl;
		}
		return true;
	};
	bool get(int id, Key key, Value value, bool direct = false) {
		// std::cout << "doing get!" << std::endl;
		Hash hash = FNV1Ahash(key) % (TABLE_INIT_SIZE / SLOT_NUM);
		Tag tag = TAGhash(key);
		KStr str;
		itos(tag, str);
		Hash hash2 = hash ^ (FNV1Ahash(str) % (TABLE_INIT_SIZE / SLOT_NUM));

		Tag ttag;
		KStr tkey;
		unsigned long long vp;
		int valuesize;
		unsigned int kv_crc32;
		unsigned int slot_crc32;

		char * databuf = RDMAdo(getDataBuf());
		bool checked[2] = {false};
		bool flag = false;
		std::vector<RPI> rpiv;
		while (!checked[0] || !checked[1]) {
		// std::cout << "hash: " << hash << ", hash2: " << hash2 << std::endl;		
			if (!checked[0]) {
				char * tmp = databuf;
				rpiv.clear();
				rpiv.push_back(RPI(baseAddr + hash * sizeof(pilafBucket), sizeof(pilafBucket), 0));
				hashfetchcount+=1;
				RDMAdo(readRemoteData(rpiv, NULL, sizeof(pilafBucket), true));
				if (rand() % 10 == 9) {
					hashfetchcount+=1;
					RDMAdo(readRemoteData(rpiv, NULL, sizeof(pilafBucket), true));
				}
				pilafBucket pb;
				pb = *(pilafBucket *)tmp;
				ttag = pb.slots[0].tag;
				memcpy(tkey, pb.slots[0].key, sizeof(KStr));
				vp = (unsigned long long) pb.slots[0].vp;
				valuesize = pb.slots[0].valuesize;
				kv_crc32 = pb.slots[0].kv_crc32;
				slot_crc32 = pb.slots[0].slot_crc32;

				if (calSlotCRC32(ttag, tkey, (char *)vp, valuesize, kv_crc32) == slot_crc32) {
					if (vp == NULL || (vp != NULL && strcmp(key, tkey) != 0))
						checked[0] = true; // flag = false;
					else {
						rpiv.clear();
						rpiv.push_back(RPI(vp, valuesize, 0));
						valuefetchcount++;
						RDMAdo(readRemoteData(rpiv, NULL, valuesize, true));
						if (CRC32((void *)databuf, valuesize, 0) == kv_crc32) {
							checked[0] = true;
							flag = true;
							break;
						}
					}
				} else std::cout << "CRC32 unmatch for check 1" << std::endl;
			}

			if (!checked[1]) {
				char * tmp = databuf;
				rpiv.clear();
				rpiv.push_back(RPI(baseAddr + hash2 * sizeof(pilafBucket), sizeof(pilafBucket), 0));
				hashfetchcount++;
				RDMAdo(readRemoteData(rpiv, NULL, sizeof(pilafBucket), true));
				if (rand() % 10 == 9) {
					hashfetchcount+=1;
					RDMAdo(readRemoteData(rpiv, NULL, sizeof(pilafBucket), true));
				}
				pilafBucket pb;
				pb = *(pilafBucket *)tmp;
				ttag = pb.slots[0].tag;
				memcpy(tkey, pb.slots[0].key, sizeof(KStr));
				vp = (unsigned long long) pb.slots[0].vp;
				valuesize = pb.slots[0].valuesize;
				kv_crc32 = pb.slots[0].kv_crc32;
				slot_crc32 = pb.slots[0].slot_crc32;

				if (calSlotCRC32(ttag, tkey, (char *)vp, valuesize, kv_crc32) == slot_crc32) {
					if (vp == NULL || (vp != NULL && strcmp(key, tkey) != 0))
						checked[1] = true; // flag = false;
					else {
						rpiv.clear();
						rpiv.push_back(RPI(vp, valuesize, 0));
						RDMAdo(readRemoteData(rpiv, NULL, valuesize, true));
						valuefetchcount++;
						if (CRC32((void *)databuf, valuesize, 0) == kv_crc32) {
							checked[1] = true;
							flag = true;
							break;
						}
					}
				} else std::cout << "CRC32 unmatch for check 2" << std::endl;
			}
		}

		if (flag) {
			opcount++;
			if (opcount >= 1000000) {
				opcount = 0;
				std::cout << "hash count: " << hashfetchcount << ", value count: " << valuefetchcount << std::endl;
			}
			return true;
		} else {
			// std::cout << "===== MSG_GET fail " << id << std::endl;
			return false;
		}
	};
	bool unset(int id, Key key) {
		// std::cout << "===== sending MSG_UNSET " << id << std::endl;
		char * bodyptr = RDMAdo(getSendBuf());
		memcpy(bodyptr, key, sizeof(Key));
		RDMAdo(sendMsg(SEND_MSG_UNSET, NULL, true));

		int msgtype;
		RDMAdo(recvMsg(msgtype, NULL, true));
		// std::cout << "===== Receiving back msg from server " << id << std::endl;
		if (msgtype == RECV_MSG_FAIL) {
			std::cout << "===== MSG_UNSET fail " << id << std::endl;
			return false;
		}
		// else std::cout << "===== MSG_UNSET succeed " << id << std::endl;

		return true;
	};
};

#endif /* FORCAKV_CLIENT_PILAF_H_ */

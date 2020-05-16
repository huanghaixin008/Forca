/*
 * forcakv-server-pilaf.h
 *
 *  Created on: May 22, 2018
 *      Author: hhx008
 */

#ifndef FORCAKV_SERVER_PILAF_H_
#define FORCAKV_SERVER_PILAF_H_

# include "forca-server.h"

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

class ForcaKVServer : public ForcaServer {
private:
	int count;
	ObjectRWLock rwlock;
public:
	ForcaKVServer() : ForcaServer(NET_MSG_SIZE, MAX_SIZE_PER_OP) {
		//pthread_mutex_init(&writelock, NULL);
		count = 0;
	};
	~ForcaKVServer() {
		//pthread_mutex_destroy(&writelock);
	};

	bool put(Key key, Value value, int vsize) {
		// pthread_mutex_lock(&writelock);
		rwlock.wrlockObject(key);
		bool flag;
		pthread_spin_lock(&regionpoollock);
		char * newregion = allocateRegion(size2power(vsize));
		pthread_spin_unlock(&regionpoollock);
		memcpy(newregion, value, vsize);
		persist((void *)newregion, vsize);
		unsigned int kv_crc32 = CRC32((void *)value, vsize, 0);
		char * region = hash.Lookup(key);
		if (region) {
			flag = hash.Update(key, newregion, vsize, kv_crc32);
			pthread_spin_lock(&regionpoollock);
			freeRegion(region);
			pthread_spin_unlock(&regionpoollock);
		} else flag = hash.Insert(key, newregion, vsize, kv_crc32);
		// pthread_mutex_unlock(&writelock);
		rwlock.unlockObject(key);
		return flag;
	};
	bool remove(Key key) {
		bool flag;
		// pthread_mutex_lock(&writelock);
		rwlock.wrlockObject(key);
		flag = hash.Delete(key);
		// pthread_mutex_unlock(&writelock);
		rwlock.unlockObject(key);
		return flag;
	};

	void process(int rid, int msgtype, char recvbuf[]) {
		// std::cout << "process() starts" << std::endl;

		Key key;
		Value value;
		bool flag;
		int len, wrid;

		char * sendbuf = RDMAdo(getSendBuf());
		char * databuf = RDMAdo(getDataBuf());
		// std::cout << "msgbuf: " << (void*)sendbuf << "databuf: " << (void*)databuf << std::endl;

		count++;
		if (count % 100000 == 0)
			std::cout << "RECV_MSG_COUNT: " << count << " " << rid << std::endl;
		// interpret parameters from msgbuf!
		char * bodyptr = recvbuf;
		switch (msgtype) {
		case SEND_MSG_SET:
			// std::cout << "receive SET msg from client" << std::endl;
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			len = *(int *)bodyptr;
			bodyptr += sizeof(int);
			memcpy(value, bodyptr, sizeof(Value));

			flag = put(key, value, len);
			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		case SEND_MSG_UNSET:
			memcpy(key, bodyptr, sizeof(Key));
			flag = remove(key);
			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		default:
			std::cout << "unknown msg!!" << std::endl;
			flag = false;
			break;
		}

		RDMAdo(sendMsg(flag ? RECV_MSG_SUCCESS : RECV_MSG_FAIL, NULL, true));
		// std::cout << "process() ends" << std::endl;
	};
};

#endif /* FORCAKV_SERVER_PILAF_H_ */

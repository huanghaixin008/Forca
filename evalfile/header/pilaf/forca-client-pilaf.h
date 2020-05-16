/*
 * forca.h
 *
 *  Created on: Mar 15, 2018
 *      Author: hhx008
 *
 *  Fast one-sided access of RDMA to NVM with high concurrency and atomicity
 */

#ifndef FORCA_H_
#define FORCA_H_

# include "network.h"

# define TABLE_INIT_SIZE (1<<17)
# define SLOT_NUM 1

struct pilafSlot {
	Tag tag;
	KStr key;
	char* vp;
	int valuesize;
	unsigned int kv_crc32;
	unsigned int slot_crc32;
};
struct pilafBucket {
	pilafSlot slots[SLOT_NUM];
};

// inherit this class to make your own client
// things to do: implement custom message filler; implement custom API
// notes: ensure parameters passed to client & server are the same
class ForcaClient {

# define RDMAdo(X) rsocket->X

protected:
	const int netMsgSize;
	const int maxSizePerOp;
	unsigned long long baseAddr;

	RDMACoordinator coordinator;
	RDMASocket * rsocket;
public:
	ForcaClient(int nms, int mspo) : netMsgSize(nms), maxSizePerOp(mspo), baseAddr(NULL), coordinator(false) {
		rsocket = NULL;
	}
	~ForcaClient() {
		delete rsocket;
	}
	void start() {
		rsocket = new RDMASocket(false, netMsgSize, maxSizePerOp);
		rsocket->init(NULL, 0);
		coordinator.messenger = rsocket;
		coordinator.start();
		rsocket->prepostRecv();
		getBaseAddr();
	}
	void getBaseAddr() {
		int msgtype, wrid;
		rsocket->sendMsg(INTERNEL_CLIENT_GETBASEADDR, NULL, true);
		wrid = rsocket->recvMsg(msgtype, NULL, true);
		char * recvbuf = RDMAdo(getRecvBuf()) + wrid * netMsgSize;
		baseAddr = *(unsigned long long *)recvbuf;
		std::cout << "Remote Base Addr: " << baseAddr << std::endl;
	}
	void exit() {
		rsocket->sendMsg(INTERNAL_CLIENT_EXIT, NULL, true);
	}
	char * getRDMASendBuf() {
		return rsocket->getSendBuf();
	}
	char * getRDMARecvBuf() {
		return rsocket->getRecvBuf();
	}
	char * getRDMADataBuf() {
		return rsocket->getDataBuf();
	}
};

#endif /* FORCA_H_ */

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

// inherit this class to make your own client
// things to do: implement custom message filler; implement custom API
// notes: ensure parameters passed to client & server are the same
class ForcaClient {

# define RDMAdo(X) rsocket->X

protected:
	const int netMsgSize;
	const int maxSizePerOp;

	RDMACoordinator coordinator;
	RDMASocket * rsocket;
public:
	ForcaClient(int nms, int mspo) : netMsgSize(nms), maxSizePerOp(mspo), coordinator(false) {
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
		// add delay to wait for server prepostRecv()
		// sleep(1);
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

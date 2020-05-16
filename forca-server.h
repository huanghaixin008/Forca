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

# include <iostream>
# include "db.h"
# include "network.h"

// implement this class to make your own server
// things to do: implement run (should be a loop); implement msg handler; etc.
// NOTE: one run thread pairs with one client (i.e one RDMASocket)
// notes: ensure parameters passed to client & server are the same
class ForcaServer {

# define RDMAdo(X) rsocketv[rid]->X
# define DBdo(X) db.X

protected:
	struct RunParams {
		ForcaServer * fcs;
		int rid;
		RunParams(ForcaServer * f, int i) : fcs(f), rid(i) {};
	};

	const int netMsgSize;
	const int maxSizePerOp;

	DB db;
	RDMACoordinator coordinator;
	std::vector<RDMASocket*> rsocketv;
	std::vector<pthread_t> rthreadv;

	// a function for handling outstanding client request
	void outstandHandlerLoop() {
		pthread_t trthread;
		while (true) {
			while (!coordinator.outstanding) {
				usleep(100000);
			}

			std::cout << "we got outstanding!" << std::endl;
			RDMASocket * nrsocket = new RDMASocket(true, netMsgSize, maxSizePerOp);
			nrsocket->init((void *) nvm->getRegionPool(), REGION_POOL_SIZE);

			rsocketv.push_back(nrsocket);
			coordinator.messenger = nrsocket;
			coordinator.outstanding = false;
			// wait for connection finishes
			std::cout << "wait for connection done..." << std::endl;
			while (coordinator.connecting) {
				usleep(1000);
			}
			std::cout << "connection done, prepostRecv()" << std::endl;
			nrsocket->prepostRecv();

			RunParams * rp = new RunParams(this, rsocketv.size()-1);
			pthread_create(&trthread, NULL, runRoutine, (void*)rp);
			pthread_detach(trthread);
		}
	};
	static void * runRoutine(void * args) {
		RunParams * rp = (RunParams *) args;
		ForcaServer * fcs = rp->fcs;
		int rid = rp->rid;
		delete rp;

		fcs->run(rid);
		return NULL;
	};
public:
	ForcaServer(int nms, int mspo) : netMsgSize(nms), maxSizePerOp(mspo), coordinator(true) {
		db.init();
	}
	virtual ~ForcaServer() {
		for (auto rsocket : rsocketv)
			delete rsocket;
	};
	void start() {
		coordinator.start();
		// start outstanding handler
		outstandHandlerLoop();
	}
	void run(int rid) {
		std::cout << "run() starts" << std::endl;

		int msgtype, wrid;

		while (true) {
			wrid = RDMAdo(recvMsg(msgtype, NULL, true));
			char * recvbuf = RDMAdo(getRecvBuf());
			switch (msgtype) {
			case INTERNAL_CLIENT_EXIT:
				std::cout << "client " << rid << " exits" << std::endl;
				delete rsocketv[rid];
				rsocketv[rid] = NULL;
				return;
			default:
				process(rid, msgtype, recvbuf);
			}
		}
	}
	virtual void process(int rid, int msgtype, char recvbuf[]) = 0;
};

#endif /* FORCA_H_ */

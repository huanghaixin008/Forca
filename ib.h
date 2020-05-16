/*
 * ib.h
 *
 *  Created on: Mar 15, 2018
 *      Author: hhx008
 */

#ifndef IB_H_
#define IB_H_

# include <infiniband/verbs.h>
# include <arpa/inet.h>
# include <sys/socket.h>
# include <netdb.h>
# include <unistd.h>
# include "util.h"

# define IB_MTU IBV_MTU_4096
# define IB_PORT 1
# define IB_SL 0
# define BATCH_MAX_NUM 32

struct IBResource {
	bool filled;
    struct ibv_context		*ctx;
    struct ibv_port_attr	 port_attr;
    struct ibv_device_attr	 dev_attr;
    IBResource() : filled(false), ctx(NULL) {}
};

struct QPInfo {
	int lid;
	int qpnum;
	int rkey;
	unsigned long long raddr;
};

struct QPFamily {
    struct ibv_context		*ctx;
    struct ibv_port_attr	 port_attr;
    struct ibv_device_attr	 dev_attr;
    struct ibv_mr	*mr;
    struct ibv_qp	*qp;
    struct ibv_cq	*cq;
    struct ibv_pd	*pd;
    void * bufaddr;
	QPInfo remoteQPinfo;
};

struct BatchInfo {
	unsigned long long raddr;
	int size;
	int offset;
	BatchInfo(unsigned long long r, int s, int o) : raddr(r), size(s), offset(o) {};
};

class IBOps {
public:
	static int createQPfamily(QPFamily * family, char * buf, unsigned long long bufsize);
	static int destroyQPfamily(QPFamily * family);

	static int setQPReady(struct ibv_qp * qp, int qpnum, int lid);
	static int postSend(struct ibv_qp * qp, int lkey, int imm_data, int size, char * buf);
	static int postRecv(struct ibv_qp * qp, int lkey, int wrid, int size, char * buf);
	static int postWriteSignaledWithimm(struct ibv_qp * qp, int lkey, int rkey, int imm_data, unsigned long long raddr, int size, char * buf);
	static int postWriteSignaled(struct ibv_qp * qp, int lkey, int rkey, unsigned long long raddr, int size, char * buf);
	static int postReadSignaled(struct ibv_qp * qp, int lkey, int rkey, unsigned long long raddr, int size, char * buf);
	static int postBatchWriteSignaled(struct ibv_qp * qp, int lkey, int rkey, std::vector<BatchInfo> batchinfo, char * buf);
	static int postBatchReadSignaled(struct ibv_qp * qp, int lkey, int rkey, std::vector<BatchInfo> batchinfo, char * buf);
	static int pollCompletion(struct ibv_cq * cq, int pollnum, std::vector<int> & wridv, std::vector<int> & immv);
};

#endif /* IB_H_ */

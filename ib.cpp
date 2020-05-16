/*
 * ib.cpp
 *
 *  Created on: Mar 15, 2018
 *      Author: hhx008
 */

# include "ib.h"
# include <unistd.h>
# include <errno.h>
# include "network.h"
# include "assert.h"

int IBOps::createQPfamily(QPFamily * family, char * buf, unsigned long long bufsize) {
	int ret;
	struct ibv_device ** dev_list = NULL;

	/* get IB device list */
	dev_list = ibv_get_device_list(NULL);
	assert(dev_list != NULL);
	/* create IB context */
	family->ctx = ibv_open_device(*dev_list);
	assert(family->ctx != NULL);
	/* query IB port attribute */
	ret = ibv_query_port(family->ctx, IB_PORT, &family->port_attr);
	assert(ret == 0);
	 /* query IB device attr */
	ret = ibv_query_device(family->ctx, &family->dev_attr);
	assert(ret == 0);
	// be cautious here
	ibv_free_device_list(dev_list);

	/* allocate protection domain */
	family->pd = ibv_alloc_pd(family->ctx);
	assert(family->pd != NULL);
	/* create cq */
	family->cq = ibv_create_cq(family->ctx, family->dev_attr.max_cqe, NULL, NULL, 0);
	assert(family->cq != NULL);
	if (buf != NULL) {
		/* register mr */
		std::cout << "[register mr]" << std::endl;
		std::cout << "! " << (void *)buf << " " << bufsize << std::endl;
		family->mr = ibv_reg_mr(family->pd, (void *)buf, bufsize,
						IBV_ACCESS_LOCAL_WRITE |
						IBV_ACCESS_REMOTE_READ |
						IBV_ACCESS_REMOTE_WRITE);
		if (family->mr == NULL)
			perror("register mr failed");
		assert(family->mr != NULL);
	}
	family->bufaddr = buf;
	/* create qp */
	struct ibv_qp_init_attr qp_init_attr;
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.send_cq = family->cq;
	qp_init_attr.recv_cq = family->cq;
	qp_init_attr.cap.max_send_wr = family->dev_attr.max_qp_wr;
	qp_init_attr.cap.max_recv_wr = family->dev_attr.max_qp_wr;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	// qp type here
	qp_init_attr.qp_type = IBV_QPT_RC;

	family->qp = ibv_create_qp(family->pd, &qp_init_attr);
	assert(family->qp != NULL);

	return 0;
}

int IBOps::destroyQPfamily(QPFamily * family) {
	ibv_destroy_qp(family->qp);
	ibv_dereg_mr(family->mr);
	ibv_destroy_cq(family->cq);
	ibv_dealloc_pd(family->pd);
	ibv_close_device(family->ctx);
}

int IBOps::setQPReady(struct ibv_qp * qp, int qpnum, int lid) {
	int ret;
	/* change QP state to INIT */
	{
		struct ibv_qp_attr qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state = IBV_QPS_INIT;
		qp_attr.pkey_index = 0;
		qp_attr.port_num = IB_PORT;
		qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
				   IBV_ACCESS_REMOTE_READ |
				   IBV_ACCESS_REMOTE_ATOMIC |
				   IBV_ACCESS_REMOTE_WRITE;

		ret = ibv_modify_qp(qp, &qp_attr,
				 IBV_QP_STATE | IBV_QP_PKEY_INDEX |
				 IBV_QP_PORT  | IBV_QP_ACCESS_FLAGS);
		assert(ret == 0);
	}
	/* Change QP state to RTR */
	{
		struct ibv_qp_attr  qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state           = IBV_QPS_RTR;
		qp_attr.path_mtu           = IB_MTU;
		qp_attr.dest_qp_num        = qpnum;
		qp_attr.rq_psn             = 0;
		qp_attr.max_dest_rd_atomic = 1;
		qp_attr.min_rnr_timer      = 12;
		qp_attr.ah_attr.is_global  = 0;
		qp_attr.ah_attr.dlid       = lid;
		qp_attr.ah_attr.sl         = IB_SL;
		qp_attr.ah_attr.src_path_bits = 0;
		qp_attr.ah_attr.port_num      = IB_PORT;

		ret = ibv_modify_qp(qp, &qp_attr,
					IBV_QP_STATE | IBV_QP_AV |
					IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
					IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
					IBV_QP_MIN_RNR_TIMER);
		assert(ret == 0);
	}
	/* Change QP state to RTS */
	{
		struct ibv_qp_attr  qp_attr;
		memset(&qp_attr, 0, sizeof(qp_attr));
		qp_attr.qp_state      = IBV_QPS_RTS;
		qp_attr.timeout       = 14;
		qp_attr.retry_cnt     = 7;
		qp_attr.rnr_retry     = 7;
		qp_attr.sq_psn        = 0;
		qp_attr.max_rd_atomic = 1;

		ret = ibv_modify_qp(qp, &qp_attr,
					 IBV_QP_STATE | IBV_QP_TIMEOUT |
					 IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
					 IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
		assert(ret == 0);
	}

	return 0;
}

int IBOps::postSend(struct ibv_qp * qp, int lkey, int imm_data, int size, char * buf) {
	int ret = 0;
	struct ibv_send_wr *bad_send_wr;

	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr   = (uintptr_t) buf;
	list.length = size;
	list.lkey   = lkey;

	struct ibv_send_wr send_wr;
	memset(&send_wr, 0, sizeof(send_wr));
	send_wr.wr_id      = 0;
	send_wr.sg_list    = &list;
	send_wr.num_sge    = 1;
	send_wr.opcode     = IBV_WR_SEND_WITH_IMM;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.imm_data   = htonl(imm_data);

	ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	assert(ret == 0);
	return ret;
}

int IBOps::postRecv(struct ibv_qp * qp, int lkey, int wrid, int size, char * buf) {
	int ret = 0;
	struct ibv_recv_wr *bad_recv_wr;

	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr   = (uintptr_t) buf;
	list.length = size;
	list.lkey   = lkey;

	struct ibv_recv_wr recv_wr;
	memset(&recv_wr, 0, sizeof(recv_wr));
	recv_wr.wr_id   = wrid;
	recv_wr.sg_list = &list;
	recv_wr.num_sge = 1;

	ret = ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
	assert(ret == 0);
	return ret;
}

int IBOps::postWriteSignaledWithimm(struct ibv_qp * qp, int lkey, int rkey, int imm_data, unsigned long long raddr, int size, char * buf) {
	int ret = 0;
	struct ibv_send_wr *bad_send_wr;

	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr   = (uintptr_t) buf;
	list.length = size;
	list.lkey   = lkey;

	struct ibv_send_wr send_wr;
	memset(&send_wr, 0, sizeof(send_wr));
	send_wr.wr_id      = 0;
	send_wr.sg_list    = &list;
	send_wr.num_sge    = 1;
	send_wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.imm_data = htonl(imm_data);
	send_wr.wr.rdma.remote_addr = raddr;
	send_wr.wr.rdma.rkey        = rkey;

	ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	assert(ret == 0);
	return ret;
}

int IBOps::postWriteSignaled(struct ibv_qp * qp, int lkey, int rkey, unsigned long long raddr, int size, char * buf) {
	int ret = 0;
	struct ibv_send_wr *bad_send_wr;

	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr   = (uintptr_t) buf;
	list.length = size;
	list.lkey   = lkey;

	struct ibv_send_wr send_wr;
	memset(&send_wr, 0, sizeof(send_wr));
	send_wr.wr_id      = 0;
	send_wr.sg_list    = &list;
	send_wr.num_sge    = 1;
	send_wr.opcode     = IBV_WR_RDMA_WRITE;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr = raddr;
	send_wr.wr.rdma.rkey        = rkey;

	ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	assert(ret == 0);
	return ret;
}

int IBOps::postReadSignaled(struct ibv_qp * qp, int lkey, int rkey, unsigned long long raddr, int size, char * buf) {
	int ret = 0;
	struct ibv_send_wr *bad_send_wr;

	struct ibv_sge list;
	memset(&list, 0, sizeof(list));
	list.addr   = (uintptr_t) buf;
	list.length = size;
	list.lkey   = lkey;

	struct ibv_send_wr send_wr;
	memset(&send_wr, 0, sizeof(send_wr));
	send_wr.wr_id      = 0;
	send_wr.sg_list    = &list;
	send_wr.num_sge    = 1;
	send_wr.opcode     = IBV_WR_RDMA_READ;
	send_wr.send_flags = IBV_SEND_SIGNALED;
	send_wr.wr.rdma.remote_addr = raddr;
	send_wr.wr.rdma.rkey        = rkey;

	ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
	assert(ret == 0);
	return ret;
}

// TODO can we aggregate wr into one by seg list?
int IBOps::postBatchWriteSignaled(struct ibv_qp * qp, int lkey, int rkey, std::vector<BatchInfo> batchinfo, char * buf) {
	struct ibv_sge sgl[BATCH_MAX_NUM];
	struct ibv_send_wr send_wr[BATCH_MAX_NUM];
	struct ibv_send_wr *bad_send_wr;
	int batchnum = batchinfo.size();

	memset(sgl, 0, BATCH_MAX_NUM * sizeof(ibv_sge));
	memset(send_wr, 0, BATCH_MAX_NUM * sizeof(ibv_send_wr));
	for (int i=0; i<batchnum; i++) {
		sgl[i].addr = (uintptr_t) (buf + batchinfo[i].offset);
		sgl[i].length = batchinfo[i].size;
		sgl[i].lkey = lkey;
		send_wr[i].sg_list = &sgl[i];
		send_wr[i].num_sge = 1;
		send_wr[i].next = (i == batchnum - 1) ? NULL : &send_wr[i+1];
		send_wr[i].wr_id = 0;
		send_wr[i].opcode = IBV_WR_RDMA_WRITE;
		send_wr[i].send_flags = (i == batchnum - 1) ? IBV_SEND_SIGNALED : 0;
		send_wr[i].wr.rdma.remote_addr = batchinfo[i].raddr;
		send_wr[i].wr.rdma.rkey = rkey;
	}

	int ret = ibv_post_send(qp, &send_wr[0], &bad_send_wr);
	assert(ret == 0);
	return ret;
}

int IBOps::postBatchReadSignaled(struct ibv_qp * qp, int lkey, int rkey, std::vector<BatchInfo> batchinfo, char * buf) {
	struct ibv_sge sgl[BATCH_MAX_NUM];
	struct ibv_send_wr send_wr[BATCH_MAX_NUM];
	struct ibv_send_wr *bad_send_wr;
	int batchnum = batchinfo.size();

	memset(sgl, 0, BATCH_MAX_NUM * sizeof(ibv_sge));
	memset(send_wr, 0, BATCH_MAX_NUM * sizeof(ibv_send_wr));
	for (int i=0; i<batchnum; i++) {
		sgl[i].addr = (uintptr_t) (buf + batchinfo[i].offset);
		sgl[i].length = batchinfo[i].size;
		sgl[i].lkey = lkey;
		send_wr[i].sg_list = &sgl[i];
		send_wr[i].num_sge = 1;
		send_wr[i].next = (i == batchnum - 1) ? NULL : &send_wr[i+1];
		send_wr[i].wr_id = 0;
		send_wr[i].opcode = IBV_WR_RDMA_READ;
		send_wr[i].send_flags = (i == batchnum - 1) ? IBV_SEND_SIGNALED : 0;
		send_wr[i].wr.rdma.remote_addr = batchinfo[i].raddr;
		send_wr[i].wr.rdma.rkey = rkey;
	}

	int ret = ibv_post_send(qp, &send_wr[0], &bad_send_wr);
	assert(ret == 0);
	return ret;
}

int IBOps::pollCompletion(struct ibv_cq * cq, int pollnum, std::vector<int> & wridv, std::vector<int> & immv) {
	int delta, count = 0;
	int success = 0;
	struct ibv_wc wc;
	while (count < pollnum) {
		delta = ibv_poll_cq(cq, 1, &wc);
		if (delta > 0) {
			count += delta;
			if (wc.status == IBV_WC_SUCCESS) {
				success++;
				wridv.push_back(wc.wr_id);
				immv.push_back(ntohl(wc.imm_data));
			} else {
				std::cout << "ibv_poll_cq get failed wc! wc.status: " << wc.status << std::endl;
				immv.push_back(INTERNAL_ERROR_FAILEDWC);
			}
		}
		// usleep(100);
	}

	return success;
}

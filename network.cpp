/*
 * network.cpp
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

# include <unistd.h>
# include "network.h"
# include "assert.h"
# include <pthread.h>
# include <errno.h>
# include <cstring>

int RDMACoordinator::socketRead(int sock_fd, void * buffer, size_t len) {
	int nr, totalread;
	char * buf = (char *) buffer;
	totalread = 0;

	while (len != 0 && (nr = read(sock_fd, buf, len)) != 0) {
		if (nr < 0) {
			if (errno == EINTR) {
				continue;
			} else assert(false);
		}
		len -= nr;
		buf += nr;
		totalread += nr;
	}

	return totalread;
}

int RDMACoordinator::socketWrite(int sock_fd, void * buffer, size_t len) {
	int nw, totalwrite;
	const char *buf = (char *) buffer;

	for (totalwrite = 0; totalwrite < len; ) {
		nw = write(sock_fd, buf, len - totalwrite);
		if (nw <= 0) {
			if (nw == -1 && errno == EINTR) {
				continue;
			} else assert(false);
		}
		totalwrite += nw;
		buf += nw;
	}

	return totalwrite;
}

int RDMACoordinator::socketCreateBind(char * port) {
	struct addrinfo hints;
	struct addrinfo * result, * rp;
	int sock_fd = -1, ret = 0;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_UNSPEC;
	hints.ai_flags = AI_PASSIVE;

	ret = getaddrinfo(NULL, port, &hints, &result);
	assert(ret == 0);

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock_fd < 0) {
			continue;
		}

		ret = bind(sock_fd, rp->ai_addr, rp->ai_addrlen);
		if (ret == 0) {
			/* bind success */
			break;
		}

		close(sock_fd);
		sock_fd = -1;
	}
	assert(rp != NULL);

	freeaddrinfo(result);
	return sock_fd;
}

int RDMACoordinator::socketCreateConnect(char * serverName, char * port) {
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sock_fd = -1, ret = 0;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    ret = getaddrinfo(serverName, port, &hints, &result);
    assert(ret == 0);

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sock_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock_fd == -1) {
            continue;
        }

        ret = connect(sock_fd, rp->ai_addr, rp->ai_addrlen);
        if (ret == 0) {
            /* connection success */
            break;
        }

        close(sock_fd);
        sock_fd = -1;
    }
    assert(rp != NULL);

    freeaddrinfo(result);
    return sock_fd;
}

int RDMACoordinator::socketGetQPInfo(int sock_fd, struct QPInfo * qpinfo) {
	int n;
	struct QPInfo tmp_qp_info;

	std::cout << "before socket read during socketGetQPInfo()" << std::endl;
	n = socketRead(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
	std::cout << "socket read " << n << " bytes during socketGetQPInfo()" << std::endl;
	assert(n == sizeof(struct QPInfo));

	qpinfo->lid       = ntohs(tmp_qp_info.lid);
	qpinfo->qpnum    = ntohl(tmp_qp_info.qpnum);
	qpinfo->rkey      = ntohl(tmp_qp_info.rkey);
	qpinfo->raddr     = ntohll(tmp_qp_info.raddr);
	return 0;
}

int RDMACoordinator::socketSetQPInfo(int sock_fd, struct QPInfo * qpinfo) {
	int n;
	struct QPInfo tmp_qp_info;

	tmp_qp_info.lid       = htons(qpinfo->lid);
	tmp_qp_info.qpnum    = htonl(qpinfo->qpnum);
	tmp_qp_info.rkey      = htonl(qpinfo->rkey);
	tmp_qp_info.raddr     = htonll(qpinfo->raddr);

	std::cout << "before socket write during socketSetQPInfo()" << std::endl;
	n = socketWrite(sock_fd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
	std::cout << "socket write " << n << " bytes during socketSetQPInfo()" << std::endl;
	assert(n == sizeof(struct QPInfo));
	return 0;
}

void RDMACoordinator::start() {
	// pthread_create(&threadArray[i], NULL, rangeSet, (void*)argsArray[i]);
	if (isServer) {
		pthread_t thread;
		pthread_create(&thread, NULL, serverListenRoutine, (void*)this);
	} else coorConnect();
}

void * RDMACoordinator::serverListenRoutine(void * args) {
	RDMACoordinator * coordinator = (RDMACoordinator *) args;
	coordinator->coorListen();
	return NULL;
}

// it's a infinite loop, in a loop only one request from client will be handled
int RDMACoordinator::coorListen() {
	int	ret = 0;
	int	sockfd = 0;
	int	peer_sockfd = 0;
	char sock_buf[64];
	struct sockaddr_in peer_addr;
	socklen_t peer_addr_len = sizeof(struct sockaddr_in);
	QPFamily * msgf, * dataf;
	RDMASocket * rsocket;
	struct QPInfo localQPinfo, remoteQPinfo;

	sockfd = socketCreateBind(SOCKET_PORT);
	assert(sockfd > 0);
	listen(sockfd, 32);

	while (true) {
		// wait for previous socket to be notified about connection completion
		usleep(100000);
		peer_sockfd = accept(sockfd, (struct sockaddr *)&peer_addr, &peer_addr_len);
		assert(peer_sockfd > 0);
		// 0. encounter outstanding client initiating request, set flag (notify handler) and wait
		connecting = true;
		outstanding = true;
		while (outstanding)
			usleep(100);
		rsocket = messenger;
		msgf = rsocket->getMsgQPFamily();
		dataf = rsocket->getDataQPFamily();

		/* connect msg qp family */
		localQPinfo.lid	 = msgf->port_attr.lid;
		localQPinfo.qpnum = msgf->qp->qp_num;
		localQPinfo.rkey   = msgf->mr->rkey;
		localQPinfo.raddr  = (unsigned long long) msgf->bufaddr;

		ret = socketGetQPInfo(peer_sockfd, &remoteQPinfo);
		assert(ret == 0);
		ret = socketSetQPInfo(peer_sockfd, &localQPinfo);
		assert(ret == 0);

		msgf->remoteQPinfo.rkey  = remoteQPinfo.rkey;
		msgf->remoteQPinfo.raddr = remoteQPinfo.raddr;
		msgf->remoteQPinfo.lid = remoteQPinfo.lid;
		msgf->remoteQPinfo.qpnum = remoteQPinfo.qpnum;
		ret = IBOps::setQPReady(msgf->qp, remoteQPinfo.qpnum,
					remoteQPinfo.lid);
		assert(ret == 0);

		memset(sock_buf, 0, sizeof(SOCKET_SYNC_MSG));
		ret = socketRead(peer_sockfd, sock_buf, sizeof(SOCKET_SYNC_MSG));
		assert(ret == sizeof(SOCKET_SYNC_MSG));
		ret = socketWrite(peer_sockfd, sock_buf, sizeof(SOCKET_SYNC_MSG));
		assert(ret == sizeof(SOCKET_SYNC_MSG));

		/* connect data qp family */
		localQPinfo.lid	 = dataf->port_attr.lid;
		localQPinfo.qpnum = dataf->qp->qp_num;
		localQPinfo.rkey   = dataf->mr->rkey;
		localQPinfo.raddr  = (unsigned long long) dataf->bufaddr;

		ret = socketGetQPInfo(peer_sockfd, &remoteQPinfo);
		assert(ret == 0);
		ret = socketSetQPInfo(peer_sockfd, &localQPinfo);
		assert(ret == 0);

		dataf->remoteQPinfo.rkey  = remoteQPinfo.rkey;
		dataf->remoteQPinfo.raddr = remoteQPinfo.raddr;
		dataf->remoteQPinfo.lid = remoteQPinfo.lid;
		dataf->remoteQPinfo.qpnum = remoteQPinfo.qpnum;
		ret = IBOps::setQPReady(dataf->qp, remoteQPinfo.qpnum,
					remoteQPinfo.lid);
		assert(ret == 0);

		memset(sock_buf, 0, sizeof(SOCKET_SYNC_MSG));
		ret = socketRead(peer_sockfd, sock_buf, sizeof(SOCKET_SYNC_MSG));
		assert(ret == sizeof(SOCKET_SYNC_MSG));
		ret = socketWrite(peer_sockfd, sock_buf, sizeof(SOCKET_SYNC_MSG));
		assert(ret == sizeof(SOCKET_SYNC_MSG));

		close(peer_sockfd);
		connecting = false;
		bool cv = connecting;
	}

	close(sockfd);
	return 0;
}

int RDMACoordinator::coorConnect() {
	int ret	= 0;
	int sockfd = 0;
	char sync_buf[10] = {0};
	memcpy(sync_buf, "sync", 4);
	char sock_buf[64];
	RDMASocket * rsocket = messenger;
	QPFamily * msgf = rsocket->getMsgQPFamily();
	QPFamily * dataf = rsocket->getDataQPFamily();
	struct QPInfo localQPinfo, remoteQPinfo;

	sockfd = socketCreateConnect(SERVER_NAME, SOCKET_PORT);
	assert(sockfd > 0);

	// 1. connect msg QPs
	localQPinfo.lid = msgf->port_attr.lid;
	localQPinfo.qpnum  = msgf->qp->qp_num;
	localQPinfo.rkey    = msgf->mr->rkey;
	localQPinfo.raddr   = (unsigned long long) msgf->bufaddr;

	/* send qp_info to server */
	ret = socketSetQPInfo(sockfd, &localQPinfo);
	assert(ret == 0);
	/* get qp_info from server */
	ret = socketGetQPInfo(sockfd, &remoteQPinfo);
	assert(ret == 0);
	/* store rkey and raddr info */
	msgf->remoteQPinfo.rkey = remoteQPinfo.rkey;
	msgf->remoteQPinfo.raddr = remoteQPinfo.raddr;
	msgf->remoteQPinfo.lid = remoteQPinfo.lid;
	msgf->remoteQPinfo.qpnum = remoteQPinfo.qpnum;
	/* change QP state to RTS */
	ret = IBOps::setQPReady(msgf->qp, remoteQPinfo.qpnum,
				remoteQPinfo.lid);
	assert(ret == 0);

	// 2. sync
	ret = socketWrite(sockfd, sync_buf, sizeof(SOCKET_SYNC_MSG));
	assert(ret == sizeof(SOCKET_SYNC_MSG));
	memset(sock_buf, 0, 64);
	ret = socketRead(sockfd, sock_buf, sizeof(SOCKET_SYNC_MSG));
	assert(ret == sizeof(SOCKET_SYNC_MSG) && strcmp(sock_buf, SOCKET_SYNC_MSG) == 0);

	// 3. connect data QPs
	localQPinfo.lid = dataf->port_attr.lid;
	localQPinfo.qpnum  = dataf->qp->qp_num;
	localQPinfo.rkey    = 0; // will never be used by server
	localQPinfo.raddr   = (unsigned long long) dataf->bufaddr;

	ret = socketSetQPInfo(sockfd, &localQPinfo);
	assert(ret == 0);
	ret = socketGetQPInfo(sockfd, &remoteQPinfo);
	assert(ret == 0);
	dataf->remoteQPinfo.rkey = remoteQPinfo.rkey;
	dataf->remoteQPinfo.raddr = remoteQPinfo.raddr;
	dataf->remoteQPinfo.lid = remoteQPinfo.lid;
	dataf->remoteQPinfo.qpnum = remoteQPinfo.qpnum;
	/* change QP state to RTS */
	ret = IBOps::setQPReady(dataf->qp, remoteQPinfo.qpnum,
				remoteQPinfo.lid);
	assert(ret == 0);

	// 4. sync & leave
	ret = socketWrite(sockfd, sync_buf, sizeof(SOCKET_SYNC_MSG));
	assert(ret == sizeof(SOCKET_SYNC_MSG));
	memset(sock_buf, 0, 64);
	ret = socketRead(sockfd, sock_buf, sizeof(SOCKET_SYNC_MSG));
	assert(ret == sizeof(SOCKET_SYNC_MSG) && strcmp(sock_buf, SOCKET_SYNC_MSG) == 0);

	close(sockfd);
	return 0;
}

void RDMASocket::init(void * _datapool, unsigned long long _poolsize) {
	datapool = _datapool;
	poolsize = _poolsize;

	IBOps::createQPfamily(&msgQP, msgbuf, netMsgSize);
	if (isServer)
		IBOps::createQPfamily(&dataQP, (char *)datapool, poolsize);
	else IBOps::createQPfamily(&dataQP, databuf, maxSizePerOp);
}

void RDMASocket::prepostRecv() {
	// pre-post recv
	for (int i=0; i<PREPOST_RECV_NUM; i++)
		IBOps::postRecv(msgQP.qp, msgQP.mr->lkey, i, netMsgSize, msgbuf);
}

int RDMASocket::sendMsg(int msgtype, const netMsg * msg, bool direct) {
	std::vector<int> wridv, immv;
	if (!direct)
		memcpy(msgbuf, msg->msgbody, netMsgSize);
	IBOps::postWriteSignaledWithimm(msgQP.qp, msgQP.mr->lkey, msgQP.remoteQPinfo.rkey, msgtype, msgQP.remoteQPinfo.raddr, netMsgSize, msgbuf);
	assert(IBOps::pollCompletion(msgQP.cq, 1, wridv, immv) == 1);
	return 0;
}

int RDMASocket::recvMsg(int & msgtype, netMsg * msg, bool direct) {
	std::vector<int> wridv, immv;
	assert(IBOps::pollCompletion(msgQP.cq, 1, wridv, immv) == 1);

	msgtype = immv[0];
	if (!direct)
		memcpy(msg->msgbody, msgbuf + wridv[0] * netMsgSize, netMsgSize);
	// post a new recv
	// TODO, repost should be handled outside if direct, for now it's safe (client issues only one op at a time)
	IBOps::postRecv(msgQP.qp, msgQP.mr->lkey, wridv[0], netMsgSize, msgbuf + wridv[0] * netMsgSize);
	return wridv[0];
}

int RDMASocket::writeRemoteData(std::vector<RPI> & rpi, char * buf, int size, bool direct) {
	if (isServer || size > maxSizePerOp)
		return -1;
	if (!direct)
		memcpy(databuf, buf, size);

	std::vector<int> wridv, immv;
	IBOps::postBatchWriteSignaled(dataQP.qp, dataQP.mr->lkey, dataQP.remoteQPinfo.rkey, rpi, databuf);
	assert(IBOps::pollCompletion(dataQP.cq, 1, wridv, immv) == 1);
	return 0;
}

int RDMASocket::readRemoteData(std::vector<RPI> & rpi, char * buf, int size, bool direct) {
	if (isServer || size > maxSizePerOp)
		return -1;

	std::vector<int> wridv, immv;
	IBOps::postBatchReadSignaled(dataQP.qp, dataQP.mr->lkey, dataQP.remoteQPinfo.rkey, rpi, databuf);
	assert(IBOps::pollCompletion(dataQP.cq, 1, wridv, immv) == 1);

	if (!direct)
		memcpy(buf, databuf, size);
	return 0;
}


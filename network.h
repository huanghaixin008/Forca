/*
 * network.h
 *
 *  Created on: Nov 15, 2017
 *      Author: hhx008
 */

#ifndef NETWORK_H_
#define NETWORK_H_

# include <iostream>
# include <arpa/inet.h>
# include <sys/socket.h>
# include <netdb.h>
# include <vector>
# include <unistd.h>
# include "util.h"
# include "ib.h"

# define SERVER_NAME "10.0.0.8"
# define SOCKET_PORT "7997"
# define SOCKET_SYNC_MSG "sync"
// should be less than infiniband MTU
# define PREPOST_RECV_NUM 32

// internal msgtype use int from -255 to 255
// user defined msgtype should not use them
enum InternalMsgType {
	INTERNAL_CLIENT_EXIT = 99,
	INTERNEL_CLIENT_GETBASEADDR,
};

enum InternalMsgError {
	INTERNAL_ERROR_FAILEDWC = -100,
};

struct netMsg {
	char * msgbody;
	netMsg(int nms) { msgbody = new char [nms]; memset(msgbody, 0, nms); };
	~netMsg() { delete msgbody; };
};

class RDMASocket;

class RDMACoordinator {
private:
	const bool isServer;

	static void * serverListenRoutine(void * args);
	// normally used by server, to wait for client connection
	int coorListen();
	// normally used by client, to connect server
	int coorConnect();
	int socketRead(int sock_fd, void * buffer, size_t len);
	int socketWrite(int sock_fd, void * buffer, size_t len);
	int socketCreateBind(char * port);
	int socketCreateConnect(char * serverName, char * port);
	int socketGetQPInfo(int sock_fd, struct QPInfo * qpinfo);
	int socketSetQPInfo(int sock_fd, struct QPInfo * qpinfo);
public:
	std::atomic<bool> outstanding;
	std::atomic<bool> connecting;
	std::atomic<RDMASocket*> messenger;

	RDMACoordinator(bool _isServer) : isServer(_isServer), outstanding(false), messenger(NULL) {};
	void start();
};

// remote position info
typedef BatchInfo RPI;

class RDMASocket {
private:
	const bool isServer;
	void * datapool;
	unsigned long long poolsize;

	const int netMsgSize;
	const int maxSizePerOp;

	QPFamily msgQP;
	QPFamily dataQP;
	char * msgbuf;
	char * databuf;

public:
	RDMASocket(bool _isServer, int nms, int mspo) : isServer(_isServer), datapool(NULL), poolsize(0),
													netMsgSize(nms), maxSizePerOp(mspo) {
		msgbuf = new char [netMsgSize];
		if (isServer)
			databuf = NULL;
		else databuf = new char [maxSizePerOp];
		std::cout << "RDMASocket, msgbuf: " << (void*)msgbuf << ", databuf: " << (void*)databuf << std::endl;
	};
	~RDMASocket() {
		IBOps::destroyQPfamily(&msgQP);
		IBOps::destroyQPfamily(&dataQP);
		if (databuf)
			delete databuf;
		delete msgbuf;
	};
	void init(void * _datapool, unsigned long long _poolsize);
	void prepostRecv();
	char * getSendBuf() { return msgbuf; };
	char * getRecvBuf() { return msgbuf; };
	char * getDataBuf() { return databuf; };
	QPFamily * getMsgQPFamily() { return &msgQP; };
	QPFamily * getDataQPFamily() { return &dataQP; };
	// send/recv & read/write interface
	int sendMsg(int msgtype, const netMsg * msg, bool direct = false);
	// may block
	int recvMsg(int & msgtype, netMsg * msg, bool direct = false);
	int writeRemoteData(std::vector<RPI> & rpi, char * buf, int size, bool direct = false);
	int readRemoteData(std::vector<RPI> & rpi, char * buf, int size, bool direct = false);
};

#endif /* NETWORK_H_ */

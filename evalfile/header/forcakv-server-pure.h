# ifndef FORCAKV_SERVER_H_
# define FORCAKV_SERVER_H_

# include "forca-server.h"
# include <sys/time.h>

# define KV_VALUE_LEN 2048
# define NET_MSG_SIZE (4096+128)
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
	unsigned long long total;
public:
	ForcaKVServer() : ForcaServer(NET_MSG_SIZE, MAX_SIZE_PER_OP) {
		count = 0;
		total = 0;
	};
	void process(int rid, int msgtype, char recvbuf[]) {
		Key key;
		Value value;
		timeval start, end;
		bool flag;
		int offset, len;

		char * sendbuf = RDMAdo(getSendBuf());
		char * databuf = RDMAdo(getDataBuf());

		count++;
		if (count == 500000) {
		 	count = 0;
			total = 0;
		}
		char * bodyptr = recvbuf;
		switch (msgtype) {
		case SEND_MSG_SET:
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;
			bodyptr += sizeof(int);
			memcpy(value, bodyptr, sizeof(Value));
			gettimeofday(&start, NULL);
			if (!DBdo(exist(key))) {
				assert(DBdo(create(key, len)));
			}
			flag = DBdo(pureWrite(key, offset, value, len));
			gettimeofday(&end, NULL);			
			total += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
			if (!flag) {
				std::cout << "SEND_MSG_SET failed, key: " << key << std::endl;	
			}

			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		case SEND_MSG_GET:
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;

			memset(sendbuf, 0, NET_MSG_SIZE);
			gettimeofday(&start, NULL);
			flag = DBdo(pureRead(key, offset, sendbuf, len));
			gettimeofday(&end, NULL);			
			total += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
			if (!flag) {
				std::cout << "SEND_MSG_GET failed, key: " << key << std::endl;	
			}
			break;
		case SEND_MSG_UNSET:
			memcpy(key, bodyptr, sizeof(Key));
			flag = DBdo(remove(key));
			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		default:
			flag = false;
			count--;
			break;
		}
		
		RDMAdo(sendMsg(flag ? RECV_MSG_SUCCESS : RECV_MSG_FAIL, NULL, true));
		// std::cout << "===== Send back msg done " << rid << std::endl;
	};
};

# endif

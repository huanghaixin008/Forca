# ifndef FORCAKV_SERVER_H_
# define FORCAKV_SERVER_H_

# include "forca-server.h"

# define KV_VALUE_LEN 1024
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
public:
	ForcaKVServer() : ForcaServer(NET_MSG_SIZE, MAX_SIZE_PER_OP) {
		count = 0;
	};
	void process(int rid, int msgtype, char recvbuf[]) {
		Key key;
		Value value;
		bool flag;
		int offset, len;
		unsigned int crc32;

		char * sendbuf = RDMAdo(getSendBuf());
		char * databuf = RDMAdo(getDataBuf());

		count++;
		if (count % 100000 == 0) {
			std::cout << "RECV_MSG_COUNT: " << count << " " << rid << std::endl;
			count = 0;
		}
		char * bodyptr = recvbuf;
		switch (msgtype) {
		case SEND_MSG_SET:
			// std::cout << "===== SEND_MSG_SET recved from " << rid << std::endl;
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;
			bodyptr += sizeof(int);
			memcpy(value, bodyptr, sizeof(Value));
			bodyptr += sizeof(Value);
			crc32 = *(unsigned int*)bodyptr;
			if (!DBdo(exist(key))) {
				assert(DBdo(create(key, len)));
			}
			flag = DBdo(write(key, offset, value, len, crc32));

			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		case SEND_MSG_GET:
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;

			memset(sendbuf, 0, NET_MSG_SIZE);
			flag = DBdo(read(key, offset, sendbuf, len));

			break;
		case SEND_MSG_UNSET:
			memcpy(key, bodyptr, sizeof(Key));
			flag = DBdo(remove(key));
			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		default:
			// std::cout << "===== UNKNOWN_MSG recved!" << std::endl;
			flag = false;
			break;
		}
		RDMAdo(sendMsg(flag ? RECV_MSG_SUCCESS : RECV_MSG_FAIL, NULL, true));
	};
};

# endif


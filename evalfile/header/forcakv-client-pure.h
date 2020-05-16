# ifndef FORCAKV_CLIENT_H_
# define FORCAKV_CLIENT_H_

# include "forca-client.h"

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

class ForcaKVClient : public ForcaClient {
public:
	ForcaKVClient() : ForcaClient(NET_MSG_SIZE, MAX_SIZE_PER_OP) {};
	bool set(int id, Key key, Value value, bool direct = false) {
		// std::cout << "===== sending MSG_SET " << id << std::endl;

		netMsg msgbuf(NET_MSG_SIZE);
		char * bodyptr = RDMAdo(getSendBuf());
		int count = 0;
		memcpy(bodyptr, key, sizeof(Key));
		bodyptr += sizeof(Key);
		count += sizeof(Key);
		*((int *)bodyptr) = 0;
		bodyptr += sizeof(int);
		count += sizeof(int);
		*((int *)bodyptr) = KV_VALUE_LEN;
		bodyptr += sizeof(int);
		memcpy(bodyptr, value, sizeof(Value));
		count += sizeof(int);
		RDMAdo(sendMsg(SEND_MSG_SET, NULL, true));
		// std::cout << "===== sending MSG_SET done " << id << std::endl;

		int msgtype;
		RDMAdo(recvMsg(msgtype, NULL, true));
		// std::cout << "===== Receiving back msg from server " << id << std::endl;
		if (msgtype == RECV_MSG_SUCCESS) {
			return true;
		} else {
			std::cout << "===== MSG_SET fails " << id  << std::endl;
			return false;
		}
	};
	bool get(int id, Key key, Value value, bool direct = false) {
		// std::cout << "===== sending MSG_GET " << id  << std::endl;

		char * bodyptr = RDMAdo(getSendBuf());
		memcpy(bodyptr, key, sizeof(Key));
		bodyptr += sizeof(Key);
		*((int *)bodyptr) = 0;
		bodyptr += sizeof(int);
		*((int *)bodyptr) = KV_VALUE_LEN;
		RDMAdo(sendMsg(SEND_MSG_GET, NULL, true));
		// std::cout << "===== sending MSG_GET done " << id  << std::endl;

		int msgtype;
		RDMAdo(recvMsg(msgtype, NULL, true));
		// std::cout << "===== Receiving back msg from server " << id  << std::endl;
		if (msgtype == RECV_MSG_SUCCESS) {
			// interpret msgbuf & do one-sided read
			memcpy(value, (char *)RDMAdo(getRecvBuf()), KV_VALUE_LEN);
			return true;
		} else {
			std::cout << "===== MSG_GET fail " << id << std::endl;
			return false;
		}
	};
	bool unset(int id, Key key) {
		// std::cout << "===== sending MSG_UNSET " << id << std::endl;
		netMsg msgbuf(NET_MSG_SIZE);
		char * bodyptr = RDMAdo(getSendBuf());
		memcpy(bodyptr, key, sizeof(Key));
		RDMAdo(sendMsg(SEND_MSG_UNSET, NULL, true));

		int msgtype;
		RDMAdo(recvMsg(msgtype, NULL, true));
		// std::cout << "===== Receiving back msg from server " << id << std::endl;
		if (msgtype == RECV_MSG_FAIL) {
			std::cout << "===== MSG_UNSET fail " << id << std::endl;
			return false;
		}
		else std::cout << "===== MSG_UNSET succeed " << id << std::endl;

		return true;
	};
};

# endif

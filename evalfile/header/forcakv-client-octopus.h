# ifndef FORCAKV_CLIENT_H_
# define FORCAKV_CLIENT_H_

# include "forca-client.h"

# define KV_VALUE_LEN 4096
# define NET_MSG_SIZE 256
# define MAX_SIZE_PER_OP 4096

typedef KStr Key;
typedef char Value[KV_VALUE_LEN];

enum sendMsgType {
	SEND_MSG_SET = 799,
	SEND_MSG_SET_DONE,
	SEND_MSG_GET,
	SEND_MSG_GET_DONE,
	SEND_MSG_UNSET,
};

enum recvMsgType {
	RECV_MSG_FAIL,
	RECV_MSG_SUCCESS,
	RECV_MSG_UNKNOWN,
};

struct RemoteRetEntry {
	bool end;
	unsigned long long raddr;
	int goffset;
	int size;
};

class ForcaKVClient : public ForcaClient {
private:
	std::vector<RemoteRetEntry> interpretRetMsg(char * msgbuf) {
		std::vector<RemoteRetEntry> RREv;
		RemoteRetEntry * tRRE = (RemoteRetEntry *) msgbuf;
		while (true) {
			if (tRRE->end)
				break;
			RREv.push_back(*tRRE);
			tRRE++;
		}
		return RREv;
	}
public:
	ForcaKVClient() : ForcaClient(NET_MSG_SIZE, MAX_SIZE_PER_OP) {};
	bool set(int id, Key key, Value value, bool direct = false) {
		// std::cout << "===== sending MSG_SET " << id << std::endl;

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
		count += sizeof(int);

		RDMAdo(sendMsg(SEND_MSG_SET, NULL, true));
		// std::cout << "===== sending MSG_SET done " << id << std::endl;

		int msgtype;
		int wrid = RDMAdo(recvMsg(msgtype, NULL, true));
		// std::cout << "===== Receiving back msg from server " << id << std::endl;
		if (msgtype == RECV_MSG_SUCCESS) {
			// interpret msgbuf & do one-sided write
			std::vector<RemoteRetEntry> RREv = interpretRetMsg(RDMAdo(getRecvBuf()));
			std::vector<RPI> rpiv;
			for (auto & RRE : RREv) {
				rpiv.push_back(RPI(RRE.raddr, RRE.size, RRE.goffset - 0));
			}

			RDMAdo(writeRemoteData(rpiv, value, KV_VALUE_LEN, direct));

			memcpy(RDMAdo(getSendBuf()), key, sizeof(Key));
			RDMAdo(sendMsg(SEND_MSG_SET_DONE, NULL, true));
			return true;
		} else {
			std::cout << "===== MSG_SET fails " << id  << std::endl;
			return false;
		}
	};
	bool get(int id, Key key, Value value, bool direct = false) {
		char * bodyptr = RDMAdo(getSendBuf());
		memcpy(bodyptr, key, sizeof(Key));
		bodyptr += sizeof(Key);
		*((int *)bodyptr) = 0;
		bodyptr += sizeof(int);
		*((int *)bodyptr) = KV_VALUE_LEN;
		RDMAdo(sendMsg(SEND_MSG_GET, NULL, true));

		int msgtype;
		int wrid = RDMAdo(recvMsg(msgtype, NULL, true));
		// std::cout << "===== Receiving back msg from server " << id  << std::endl;
		if (msgtype == RECV_MSG_SUCCESS) {
			// interpret msgbuf & do one-sided read
			std::vector<RemoteRetEntry> RREv = interpretRetMsg(RDMAdo(getRecvBuf()));
			std::vector<RPI> rpiv;
			for (auto & RRE : RREv) {
				rpiv.push_back(RPI(RRE.raddr, RRE.size, RRE.goffset - 0));
			}

			RDMAdo(readRemoteData(rpiv, value, KV_VALUE_LEN, direct));
			// std::cout << "===== MSG_GET succeeds " << id << std::endl;

			memcpy(RDMAdo(getSendBuf()), key, sizeof(Key));
			RDMAdo(sendMsg(SEND_MSG_GET_DONE, NULL, true));
			return true;
		} else {
			std::cout << "===== MSG_GET fail " << id << std::endl;
			return false;
		}
	};
	bool unset(int id, Key key) {
		// std::cout << "===== sending MSG_UNSET " << id << std::endl;
		char * bodyptr = RDMAdo(getSendBuf());
		memcpy(bodyptr, key, sizeof(Key));
		RDMAdo(sendMsg(SEND_MSG_UNSET, NULL, true));

		int msgtype;
		RDMAdo(recvMsg(msgtype, NULL, true));
		if (msgtype == RECV_MSG_FAIL) {
			std::cout << "===== MSG_UNSET fail " << id << std::endl;
			return false;
		}

		return true;
	};
	void unlock() {
		RDMAdo(sendMsg(SEND_MSG_SET_DONE, NULL, true));
	};	
};

# endif

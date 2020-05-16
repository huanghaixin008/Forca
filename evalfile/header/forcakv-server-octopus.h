# ifndef FORCAKV_SERVER_H_
# define FORCAKV_SERVER_H_

# include "forca-server.h"

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

class ForcaKVServer : public ForcaServer {
private:
	bool locked[2048];
	bool type[2048];
	Key keys[2048];
	int count;
public:
	ForcaKVServer() : ForcaServer(NET_MSG_SIZE, MAX_SIZE_PER_OP) {
		count = 0;
		for (int i=0; i<2048; i++)
			locked[i] = false;
	};
	void process(int rid, int msgtype, char recvbuf[]) {
		Key key;
		bool flag;
		int offset, len;
		std::vector<OPI> opiv;
		RemoteRetEntry * tRRE;

		char * sendbuf = RDMAdo(getSendBuf());
		char * databuf = RDMAdo(getDataBuf());

		count++;
		
		bool unknown = false;
		bool ret = true;
		char * bodyptr = recvbuf;
		
		if (locked[rid]) {
			memcpy(key, keys[rid], sizeof(Key));
			if (type)
				DBdo(finishRead(key));
			else DBdo(finishWrite(key));
			locked[rid] = false;
		}
		switch (msgtype) {
		case SEND_MSG_SET:
			// std::cout << "===== SEND_MSG_SET recved from " << rid << std::endl;
			
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;
			bodyptr += sizeof(int);

			// std::cout << "internal db ops begin: " << rid << std::endl;
			if (!DBdo(exist(key))) {
				assert(DBdo(create(key, len)));
			}
			flag = DBdo(inplaceLazyWrite(key, offset, len, opiv));
			if (!flag)
				std::cout << "fuck SET" << std::endl;
			else {
				locked[rid] = flag;
				type[rid] = false;
				memcpy(keys[rid], key, sizeof(Key));
			}
			// std::cout << "internal db ops end: " << rid << std::endl;

			memset(sendbuf, 0, NET_MSG_SIZE);
			tRRE = (RemoteRetEntry *)sendbuf;
			for (int i=0; i<opiv.size(); i++) {
				tRRE->end = false;
				tRRE->raddr = (unsigned long long)(opiv[i].region);
				tRRE->goffset = opiv[i].offset;
				tRRE->size = opiv[i].size;
				tRRE++;
			}
			tRRE->end = true;
			break;
		case SEND_MSG_GET:
			// std::cout << "===== SEND_MSG_GET recved from " << rid << std::endl;
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;

			flag = DBdo(inplaceLazyRead(key, offset, len, opiv));
			if (!flag)
				std::cout << "fuck GET" << std::endl;
			else {
				locked[rid] = flag;
				type[rid] = true;
				memcpy(keys[rid], key, sizeof(Key));
			}

			memset(sendbuf, 0, NET_MSG_SIZE);
			tRRE = (RemoteRetEntry *)sendbuf;
			for (int i=0; i<opiv.size(); i++) {
				tRRE->end = false;
				tRRE->raddr = (unsigned long long)(opiv[i].region);
				tRRE->goffset = opiv[i].offset;
				tRRE->size = opiv[i].size;
				tRRE++;
			}
			tRRE->end = true;
			break;
		case SEND_MSG_SET_DONE:
			ret = false;
			break;
		case SEND_MSG_GET_DONE:
			ret = false;
			break;
		case SEND_MSG_UNSET:
			memcpy(key, bodyptr, sizeof(Key));
			flag = DBdo(remove(key));
			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		default:
			std::cout << "===== UNKNOWN_MSG recved!" << std::endl;
			flag = false;
			unknown = true;
			break;
		}

		if (ret) {
			RDMAdo(sendMsg(flag ? RECV_MSG_SUCCESS : RECV_MSG_FAIL, NULL, true));
		}
	};
};

# endif

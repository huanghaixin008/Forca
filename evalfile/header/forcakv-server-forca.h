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
	SEND_MSG_GET,
	SEND_MSG_UNSET,
};

enum recvMsgType {
	RECV_MSG_FAIL,
	RECV_MSG_SUCCESS,
};

struct RemoteRetEntry {
	bool end;
	unsigned long long raddr;
	int goffset;
	int size;
};

class ForcaKVServer : public ForcaServer {
private:
	int count;
public:
	ForcaKVServer() : ForcaServer(NET_MSG_SIZE, MAX_SIZE_PER_OP) {
		count = 0;
	};
	void process(int rid, int msgtype, char recvbuf[]) {
		// std::cout << "process() starts" << std::endl;

		Key key;
		bool flag;
		int offset, len, wrid;
		unsigned int crc32;
		std::vector<OPI> opiv;
		RemoteRetEntry * tRRE;

		char * sendbuf = RDMAdo(getSendBuf());
		char * databuf = RDMAdo(getDataBuf());

		count++;
		if (count % 1000000 == 0) {
			std::cout << DBdo(getObjectUsage()) << "," << DBdo(getMetaUsage()) << "," << DBdo(getDataUsage()) << "," << DBdo(getRegionUsage()) << std::endl;
		}
		// interpret parameters from msgbuf!
		char * bodyptr = recvbuf;
		switch (msgtype) {
		case SEND_MSG_SET:
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;
			bodyptr += sizeof(int);
			crc32 = *(unsigned int *)bodyptr;

			if (!DBdo(exist(key)))
				assert(DBdo(create(key, len)));
			flag = DBdo(lazyWrite(key, offset, len, crc32, opiv));

			// set return values
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
			memcpy(key, bodyptr, sizeof(Key));
			bodyptr += sizeof(Key);
			offset = *(int *)bodyptr;
			bodyptr += sizeof(int);
			len = *(int *)bodyptr;

			flag = DBdo(lazyRead(key, offset, len, opiv));

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
		case SEND_MSG_UNSET:
			memcpy(key, bodyptr, sizeof(Key));
			flag = DBdo(remove(key));
			memset(sendbuf, 0, NET_MSG_SIZE);
			break;
		default:
			flag = false;
			break;
		}

		RDMAdo(sendMsg(flag ? RECV_MSG_SUCCESS : RECV_MSG_FAIL, NULL, true));
	};
};

# endif

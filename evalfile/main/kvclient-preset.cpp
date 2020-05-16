# include <iostream>
# include <string>
# include <cstring>
# include <sstream>
# include <cstdlib>
# include <ctime>
# include <time.h>
# include <unordered_map>
# include <pthread.h>
# include <fstream>

# include "forcakv-client.h"

# define KEY_RANGE 100000
# define OPS_NUM 10000000

using namespace std;

int main(int argc, char* argv[]) {
	bool flag;
	int base = 0;
	if (argc == 1) {
		cout << "Hello World!" << endl;
	} else {
		sscanf(argv[1], "%d", &base);
	}

	ForcaKVClient kvclient;
	char * databuf = kvclient.getRDMADataBuf();
	cout << "databuf: " << (void *)databuf << std::endl;

	for (int i=0; i<KV_VALUE_LEN; i++)
		databuf[i] = 'q';

	// PRESET: put KEY_RANGE amount of KV pairs first
	Key key;
	for (int i=0; i<KEY_RANGE; i++) {
		itos(i, key);
		kvclient.set(i, key, NULL, true);
	}

	return 0;
}

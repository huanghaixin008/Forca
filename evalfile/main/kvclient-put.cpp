# include <iostream>
# include <string>
# include <cstring>
# include <sstream>
# include <cstdlib>
# include <ctime>
# include <time.h>
# include <sys/time.h>
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

	timeval start, end;
	srand((unsigned)time(NULL));
	int count = 0;
	Key key;
	gettimeofday(&start, NULL);
	while (count < OPS_NUM) {
		count++;
		if (count % 100000 == 0)
			std::cout << "ops count: " << count << std::endl;
		memset(key, 0, sizeof(Key));
		itos(rand() % KEY_RANGE, key);
		kvclient.set(i, key, NULL, true);
	}
	gettimeofday(&end, NULL);

	cout << 1000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000 << "ms" << endl;

	return 0;
}

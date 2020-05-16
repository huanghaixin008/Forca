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

# include "forcakv-server.h"

# define KEY_RANGE 100000
# define OPS_NUM 10000000

using namespace std;

int main(int argc, char* argv[]) {
	bool flag;
	int base;
	if (argc == 1) {
		cout << "Hello World!" << endl;
	} else {
		for (int i=1; i<argc-1; i++) {
			cout << argv[i+1] << endl;
		}
		cout << "Done!" << endl;
	}

	// server
	ForcaKVServer kvserver;
	kvserver.start();

	return 0;
}

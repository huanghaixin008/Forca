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
# include <math.h>
# include "assert.h" 

# include "forcakv-client.h"

# define KEY_RANGE 100000
# define OPS_NUM 10000000

using namespace std;

inline double fastPow(double a, double b) {
  union {
    double d;
    int x[2];
  } u = { a };
  u.x[1] = (int)(b * (u.x[1] - 1072632447) + 1072632447);
  u.x[0] = 0;
  return u.d;
}

//=========================================================================
//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
//=========================================================================
double rand_val(int seed)
{
  const long  a =      16807;  // Multiplier
  const long  m = 2147483647;  // Modulus
  const long  q =     127773;  // m div a
  const long  r =       2836;  // m mod a
  static long x;               // Random int value
  long        x_div_q;         // x divided by q
  long        x_mod_q;         // x modulo q
  long        x_new;           // New x value

  // Set the seed if argument is non-zero and then return zero
  if (seed > 0)
  {
    x = seed;
    return(0.0);
  }

  // RNG using integer arithmetic
  x_div_q = x / q;
  x_mod_q = x % q;
  x_new = (a * x_mod_q) - (r * x_div_q);
  if (x_new > 0)
    x = x_new;
  else
    x = x_new + m;

  // Return a random value between 0.0 and 1.0
  return((double) x / m);
}

//===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
int zipf(double alpha, int n)
{
  static int first = true;      // Static first time flag
  static double c = 0;          // Normalization constant
  static double *sum_probs;     // Pre-calculated sum of probabilities
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int    i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds

  // Compute normalization constant on first call only
  if (first == true)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / fastPow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = (double *) malloc((n+1)*sizeof(*sum_probs));
    sum_probs[0] = 0;
    for (i=1; i<=n; i++) {
      sum_probs[i] = sum_probs[i-1] + c / fastPow((double) i, alpha);
    }
    first = false;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = rand_val(0);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n, mid;
  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return (zipf_value);
}

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

	rand_val((int)time(NULL));
	srand((unsigned)time(NULL));
	int count = 0;
	Key key;
	while (count < OPS_NUM) {
		count++;
		if (count % 100000 == 0)
			std::cout << base << ": ops count: " << count << std::endl;

		memset(key, 0, sizeof(Key));
		itos(zipf(1.5, KEY_RANGE), key);
		if (rand() % 100 >= 95)
			kvclient.set(count, key, NULL, true);
		else kvclient.get(count, key, NULL, true);
	}

	return 0;
}

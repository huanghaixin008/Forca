/*
 * allocator.h
 *
 *  Created on: Apr 15, 2018
 *      Author: hhx008
 */

#ifndef ALLOCATOR_H_
#define ALLOCATOR_H_

# include <atomic>
# include <pthread.h>

// link list table to allocate 2^n (n = 0, 1, ...) blocks
class BlockAllocator {
private:
	struct node {
		int id;
		char power;
		bool free;
		node * prev, * next;
		node() : id(0), power(0), free(true), prev(NULL), next(NULL) {};
		node(int i, char pow, node * p, node * n) :
			id(i), power(pow), free(true), prev(p), next(n) {};
	};

	bool initialized;
	const int maxAllocPower;
	const int allocRange;
	node ** allocFreelist;
	node * allocNodes;

	std::atomic<unsigned long long> usage;

	inline void deleteNode(node * n);
	inline void insertNode(node * head, node * n);
	// breakdown to create a block of 'power'
	bool breakdown(int power);
	node* merge(node * n);
public:
	BlockAllocator(int map, int ar);
	~BlockAllocator();
	// markUsed() must be called before initFreelist()!
	void markUsed(int id, int power);
	void initFreelist();
	int allocate(int power);
	void free(int id);
	
	unsigned long long getUsage() { return usage.load(); };
	void printList();
	void printNodes();
};

#endif /* ALLOCATOR_H_ */

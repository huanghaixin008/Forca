/*
 * allocator.cpp
 *
 *  Created on: Apr 15, 2018
 *      Author: hhx008
 */

# include <iostream>
# include "allocator.h"
# include "assert.h"

inline void BlockAllocator::deleteNode(node * n) {
	node * prev = n->prev;
	node * next = n->next;
	prev->next = next;
	next->prev = prev;
	n->next = n->prev = NULL;
}

inline void BlockAllocator::insertNode(node * head, node * n) {
	node * tmp = head->next;
	n->prev = head;
	head->next = n;
	n->next = tmp;
	tmp->prev = n;
	assert(n->prev && n->next);
}

BlockAllocator::BlockAllocator(int map, int r) : maxAllocPower(map), allocRange(r) {
	assert(r % (1 << map) == 0);
	allocFreelist = new node* [maxAllocPower+1];
	for (int i=0; i<=maxAllocPower; i++) {
		allocFreelist[i] = new node();
		allocFreelist[i]->prev = allocFreelist[i];
		allocFreelist[i]->next = allocFreelist[i];
	}

	allocNodes = new node [allocRange];
	for (int i=0; i<allocRange; i++) {
		allocNodes[i].id = i;
		allocNodes[i].power = -1;
		allocNodes[i].free = true;
		allocNodes[i].prev = allocNodes[i].next = NULL;
	}
	
	usage = 0;
	// init freelist
	initialized = false;
};

BlockAllocator::~BlockAllocator() {
	delete [] allocNodes;
	for (int i=0; i<=maxAllocPower; i++)
		delete allocFreelist[i];
	delete [] allocFreelist;
};

bool BlockAllocator::breakdown(int power) {
	if (power == 0 || power > maxAllocPower)
		return false;

	if (allocFreelist[power]->next == allocFreelist[power])
		if (!breakdown(power + 1))
			return false;
	assert(allocFreelist[power]->next != allocFreelist[power]);
	node * first = allocFreelist[power]->next;
	node * second = first + (1 << first->power)/2;
	// delete first from freelist[power]
	deleteNode(first);

	first->power--;
	second->power = first->power;
	second->free = true;
	// add first & second to freelist[power-1]
	insertNode(allocFreelist[power-1], second);
	insertNode(allocFreelist[power-1], first);

	return true;
}

BlockAllocator::node* BlockAllocator::merge(node * n) {
	node * buddy, * first = n;
	bool flag;
	int fsize;
	while (first->power < maxAllocPower) {
		// indicate it's first buddy or second buddy
		fsize = 1 << first->power;
		if ((first->id / fsize) % 2)
			flag = false;
		else flag = true;
		if (flag)
			buddy = first + fsize;
		else buddy = first - fsize;
		// buddy merge-able
		if (buddy->free && buddy->power == first->power) {
			// delete n & its buddy from freelist
			deleteNode(buddy);
			// generate new region & insert
			// buddy is second
			if (flag) {
				buddy->power = -1;
				first->power++;
			} else { // buddy is first
				first->power = -1;
				buddy->power++;
				first = buddy;
			}
		} else break;
	}

	return first;
};

void BlockAllocator::initFreelist() {
	assert(!initialized);
	if (initialized)
		return;
	// init freelist
	node * head = allocFreelist[maxAllocPower];
	int plus = 1 << maxAllocPower;
	for (int i=0; i<allocRange; i+=plus) {
		if (allocNodes[i].power < 0)
			allocNodes[i].power = maxAllocPower;
	}

	for (int i=0; i < allocRange; i++) {
		if (allocNodes[i].power >= 0 && allocNodes[i].free)
			insertNode(allocFreelist[allocNodes[i].power], &allocNodes[i]);
	}
	initialized = true;
};

void BlockAllocator::markUsed(int id, int power) {
	assert(!initialized);
	if (initialized)
		return;

	int tpower = power;
	node * buddy, * first = allocNodes + id;
	bool flag;
	int fsize;

	first->power = power;
	first->free = false;
	while (tpower < maxAllocPower) {
		// indicate it's first buddy or second buddy
		fsize = 1 << tpower;
		if ((first->id / fsize) % 2)
			flag = false;
		else flag = true;
		if (flag)
			buddy = first + fsize;
		else buddy = first - fsize;
		if (buddy->power >= 0)
			break;
		// buddy's unmarked
		else buddy->power = tpower;
		if (!flag)
			buddy = first;
		tpower++;
	}
	// usage += (1 << power);
};

int BlockAllocator::allocate(int power) {
	assert(initialized);
	if (!initialized)
		return -1;
	if (allocFreelist[power]->next == allocFreelist[power])
		if (!breakdown(power + 1))
			return -1;
	assert(allocFreelist[power]->next != allocFreelist[power]);
	node * tmp = allocFreelist[power]->next;
	assert(tmp);
	tmp->free = false;
	deleteNode(tmp);
	return tmp->id;
};

void BlockAllocator::free(int id) {
	assert(initialized);
	if (!initialized)
		return;
	node * tmp = allocNodes + id;
	int power = tmp->power;
	assert(!tmp->free);
	tmp->free = true;
	// add id back to freelist
	node * finaln = merge(tmp);
	insertNode(allocFreelist[finaln->power], finaln);
	// usage -= (1 << power);
};

void BlockAllocator::printList() {
	for (int i=0; i<=maxAllocPower; i++) {
		std::cout << "===== [" << i << "] =====" << std::endl;
		node * tmp = allocFreelist[i]->next;
		while (tmp != allocFreelist[i]) {
			std::cout << "id: " << tmp->id << ", prev:" << tmp->prev->id << ", power: " << tmp->power << std::endl;
			tmp = tmp->next;
		}
	}
};

void BlockAllocator::printNodes() {
	for (int i=0; i<allocRange; i++) {
		if (allocNodes[i].free)
			std::cout << allocNodes[i].power;
		else std::cout << "F";
	}
	std::cout << std::endl;
};



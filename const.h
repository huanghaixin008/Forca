/*
 * const.h
 *
 *  Created on: Nov 20, 2017
 *      Author: hhx008
 */

#ifndef CONST_H_
#define CONST_H_

# define REGIONBLOCK_SIZE 256UL
# define OBJECT_KEY_LEN 128
# define OBJECT_META_SIZE sizeof(struct metablock)
# define OBJECT_NODE_SIZE sizeof(struct object)

# define OBJECT_NUM_PERSEG 256
# define SEG_BITMAP_SIZE (OBJECT_NUM_PERSEG / 8)
# define SEGMENT_SIZE (OBJECT_NUM_PERSEG * OBJECT_META_SIZE + sizeof(void *) + SEG_BITMAP_SIZE)

#endif /* CONST_H_ */

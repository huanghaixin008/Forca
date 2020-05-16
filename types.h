/*
 * types.h
 *
 *  Created on: Nov 20, 2017
 *      Author: hhx008
 */

#ifndef TYPES_H_
#define TYPES_H_

# include "const.h"

// nvm.h
// in NVM pointer (offset)
typedef long long NVMP;

// object.h
typedef unsigned char BYTE;
typedef char KStr[OBJECT_KEY_LEN]; // 128 bytes

#endif /* TYPES_H_ */

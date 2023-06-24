/*-------------------------------------------------------------------------
 *
 * execute.h
 *
 * src/include/sdb/execute.h
 *
 *
 *-------------------------------------------------------------------------
 */


#ifndef SDB_EXECUTE_sfaew_H
#define SDB_EXECUTE_sfaew_H

#include "cdb/tupchunklist.h"

#ifdef __cplusplus
extern "C" {
#endif

void 
SDBInitRecvStream(void* task,
				  int32 motionId,
				  int32 numSegs);

void 
SDBSetupRecvStream(void* task,
				   int32 motionId,
				   int32_t from_slice,
				   int32_t to_slice,
				   int32_t from_segindex,
				   int32_t to_segindex,
				   int32_t from_route,
int32_t to_route);

void 
SDBInitSendStream(void* task,
				  int32 motionId,
int32 numSegs);

void 
SDBSetupSendStream(void* task,
				   int32 motionId,
				   int32_t from_slice,
				   int32_t to_slice,
				   int32_t from_segindex,
				   int32_t to_segindex,
				   int32_t from_route,
int32_t to_route);

bool
SDBStartSendStream(void* task,
				   int32 motionId,
int32 route);

bool 
SDBSendChunk(void *task, 
			 TupleChunkListItem tcItem, 
			 int16 motionId,
			 int16 targetRoute);

bool 
SDBBroadcastChunk(void *task, 
			 TupleChunkListItem tcItem, 
			 int16 motionId);



TupleChunkListItem 
SDBRecvTupleChunkFrom(void *task, 
					  int16 motNodeID, 
int16 srcRoute);

TupleChunkListItem
SDBRecvTupleChunkFromAny(void *task,
						 int16 motNodeID,
int16 *srcRoute);

void 
SDBBroadcastStopMessage(void* task,
						int32 motNodeId);

void
SDBSendEndOfStream(void *task,
				   int motNodeID,
				   TupleChunkListItem tcItem);


#ifdef __cplusplus
};
#endif

#endif

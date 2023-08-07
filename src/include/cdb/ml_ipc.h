/*-------------------------------------------------------------------------
 * ml_ipc.h
 *	  Motion Layer IPC Layer.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/ml_ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ML_IPC_H
#define ML_IPC_H

#include "cdb/cdbselect.h"
#include "cdb/cdbinterconnect.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"

struct SliceTable;                          /* #include "nodes/execnodes.h" */
struct EState;                              /* #include "nodes/execnodes.h" */

/* listener filedescriptors */
extern int		TCP_listenerFd;
extern int		UDP_listenerFd;

/*
 * Registration message
 *
 * Upon making a connection, the sender sends a registration message to
 * identify itself to the receiver.  A lot of the fields are just there
 * for validity checking.
 */
typedef struct RegisterMessage
{
	int32       msgBytes;
	int32       recvSliceIndex;
	int32       sendSliceIndex;
	int32       srcContentId;
	int32       srcListenerPort;
	int32       srcPid;
	int32       srcSessionId;
	int32       srcCommandCount;
} RegisterMessage;

/* 2 bytes to store the size of the entire packet.	a packet is composed of
 * of one or more serialized TupleChunks (each of which has a TupleChunk
 * header.
 */
#define PACKET_HEADER_SIZE 4

/* Performs initialization of the MotionLayerIPC.  This should be called before
 * any work is performed through functions here.  Generally, this should only
 * need to be called only once during process startup.
 *
 * Errors are indicated by calls to ereport(), and are therefore not indicated
 * by a return code.
 *
 */
extern void InitMotionLayerIPC(void);

/* Performs any cleanup necessary by the Motion Layer IPC.	This is the cleanup
 * function that matches InitMotionLayerIPC, it should only be called during
 * shutdown of the process. This includes shutting down the Motion Listener.
 *
 * Errors are indicated by calls to ereport(), and are therefore not indicated
 * in the return code.
 */
extern void CleanUpMotionLayerIPC(void);

/*
 * Wait interconnect thread to quit, called when proc exit.
 */
extern void WaitInterconnectQuit(void);

/*
 * checkForCancelFromQD
 * 		Check for cancel from QD.
 *
 * Should be called only inside the dispatcher
 */
void
checkForCancelFromQD(ChunkTransportState *pTransportStates);

/* The SetupInterconnect() function should be called at the beginning of
 * executing any DML statement that will need to use the interconnect.
 *
 * This function goes through the slicetable and makes any appropriate
 * outgoing connections as well as accepts any incoming connections.  Incoming
 * connections will have a "Register" message from them to see which remote
 * CdbProcess sent it.
 *
 * So this function essentially performs all of the setup the interconnect has
 * to perform for all of the motion nodes in the upcoming DML statement.
 *
 * PARAMETERS
 *
 *	 mySliceTable - slicetable structure that correlates to the upcoming DML
 *					statement.
 *
 *	 mySliceId - the index of the slice in the slicetable that we are a member of.
 *
 */
extern void SetupInterconnect(struct EState *estate);

/* The TeardownInterconnect() function should be called at the end of executing
 * a DML statement to close down all socket resources that were setup during
 * SetupInterconnect().
 *
 * NOTE: it is important that TeardownInterconnect() happens
 *		 regardless of the outcome of the statement. i.e. gets called
 *		 even if an ERROR occurs during the statement. For abnormal
 *		 statement termination we can force an end-of-stream notification.
 *
 */
extern void TeardownInterconnect(struct EState *estate,
								 bool hasErrors);

extern void WaitInterconnectQuit(void);


/* Sends a tuple chunk from the Postgres process to the local AMS process via
 * IPC.  This function does not block; if the IPC channel cannot accept the
 * tuple chunk for some reason, then this is indicated by a return-code.
 *
 * Errors are indicated by calls to ereport(), and are therefore not indicated
 * in the return code.
 *
 *
 * PARAMETERS:
 *	 - motNodeID:	motion node Id that the tcItem belongs to.
 *	 - targetRoute: route to send this tcItem out over.
 *	 - tcItem:		The tuple-chunk data to send.
 *
 */
extern bool SendTupleChunkToAMS(MotionLayerState *mlStates,
								void *task, 
								int16 motNodeID, 
								int16 targetRoute, 
								TupleChunkListItem tcItem);

/* The SendEosToAMS() function is used to send an "End Of Stream" message to
 * all connected receivers (generally this is a broadcast)
 *
 * PARAMETERS:
 *	 - motNodeID:	motion node Id that the tcItem belongs to.
 *	 - tcItem:		The tuple-chunk data to send.
 *
 */
extern void SendEosToAMS(MotionLayerState *mlStates,
						 void *task, 
						 int motNodeID, 
						 TupleChunkListItem tcItem);

#define ML_CHECK_FOR_INTERRUPTS(teardownActive) \
		do {if (!teardownActive && InterruptPending) CHECK_FOR_INTERRUPTS(); } while (0)

extern char *format_sockaddr(struct sockaddr_storage *sa, char *buf, size_t len);

extern void
SetupStream(EState *estate);

extern void
TeardownStreams(EState *estate);


#endif   /* ML_IPC_H */

/*-------------------------------------------------------------------------
 * ic_common.c
 *	   Interconnect code shared between UDP, and TCP IPC Layers.
 *
 * Portions Copyright (c) 2005-2008, Greenplum
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/motion/ic_common.c
 *
 * Reviewers: jzhang, tkordas
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/ip.h"
#include "nodes/execnodes.h"	/* ExecSlice, SliceTable */
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "cdb/ml_ipc.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "sdb/execute.h"

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>

/*
  #define AMS_VERBOSE_LOGGING
*/

/*=========================================================================
 * STRUCTS
 */
typedef struct interconnect_handle_t
{
	ChunkTransportState	*interconnect_context; /* Interconnect state */

	ResourceOwner owner;	/* owner of this handle */
	struct interconnect_handle_t *next;
	struct interconnect_handle_t *prev;
} interconnect_handle_t;

/*=========================================================================
 * GLOBAL STATE VARIABLES
 */

/* Socket file descriptor for the listener. */
int			TCP_listenerFd;
int			UDP_listenerFd;

static interconnect_handle_t *open_interconnect_handles;
static bool interconnect_resowner_callback_registered;

/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */

static void interconnect_abort_callback(ResourceReleasePhase phase,
								   bool isCommit,
								   bool isTopLevel,
								   void *arg);
static void cleanup_interconnect_handle(interconnect_handle_t *h);
static interconnect_handle_t *allocate_interconnect_handle(void);
static void destroy_interconnect_handle(interconnect_handle_t *h);
static interconnect_handle_t *find_interconnect_handle(ChunkTransportState *icContext);

static void
logChunkParseDetails(MotionConn *conn, uint32 ic_instance_id)
{
	struct icpkthdr *pkt;

	Assert(conn != NULL);
	Assert(conn->pBuff != NULL);

	pkt = (struct icpkthdr *) conn->pBuff;

	elog(LOG, "Interconnect parse details: pkt->len %d pkt->seq %d pkt->flags 0x%x conn->active %d conn->stopRequest %d pkt->icId %d my_icId %d",
		 pkt->len, pkt->seq, pkt->flags, conn->stillActive, conn->stopRequested, pkt->icId, ic_instance_id);

	elog(LOG, "Interconnect parse details continued: peer: srcpid %d dstpid %d recvslice %d sendslice %d srccontent %d dstcontent %d",
		 pkt->srcPid, pkt->dstPid, pkt->recvSliceIndex, pkt->sendSliceIndex, pkt->srcContentId, pkt->dstContentId);
}
/*=========================================================================
 * VISIBLE FUNCTIONS
 */

/* See ml_ipc.h */
void
InitMotionLayerIPC(void)
{
	uint16		tcp_listener = 0;
	uint16		udp_listener = 0;

	Gp_listener_port = (udp_listener << 16) | tcp_listener;

	elog(DEBUG1, "Interconnect listening on tcp port %d udp port %d (0x%x)", tcp_listener, udp_listener, Gp_listener_port);
}

/* See ml_ipc.h */
void
CleanUpMotionLayerIPC(void)
{
	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG3, "Cleaning Up Motion Layer IPC...");

	/* be safe and reset global state variables. */
	Gp_listener_port = 0;
	TCP_listenerFd = -1;
	UDP_listenerFd = -1;
}

/* See ml_ipc.h */
bool
SendTupleChunkToAMS(MotionLayerState *mlStates,
					void *task,
					int16 motNodeID,
					int16 targetRoute,
					TupleChunkListItem tcItem)
{
	int			i,
				recount = 0;
	TupleChunkListItem currItem;


#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG3, "sendtuplechunktoams: calling get_transport_state"
		 "w/transportStates %p transportState->size %d motnodeid %d route %d",
		 transportStates, transportStates->size, motNodeID, targetRoute);
#endif

	/*
	 * tcItem can actually be a chain of tcItems.  we need to send out all of
	 * them.
	 */
	for (currItem = tcItem; currItem != NULL; currItem = currItem->p_next)
	{
#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "SendTupleChunkToAMS: chunk length %d", currItem->chunk_length);
#endif

		elog(WARNING, "SendTupleChunkToAMS: chunk length %d", currItem->chunk_length);
		if (targetRoute == BROADCAST_SEGIDX)
		{
			//doBroadcast(transportStates, pEntry, currItem, &recount);
			SDBBroadcastChunk(task, currItem, motNodeID);
		}
		else
		{
			if (targetRoute < 0)
			{
				elog(FATAL, "SendTupleChunkToAMS: targetRoute is %d, must >= 0 .",
							targetRoute);
			}
			SDBSendChunk(task, currItem, motNodeID, targetRoute);
			/* in 4.0 logical mirror xmit eliminated. */
		}
	}

	/* if we found an active connection we're not done */
	// return (i < pEntry->numConns);
	return true;
}

extern void
SetupStream(EState *estate);

extern void
TeardownStreams(EState *estate);

void
SetupInterconnect(EState *estate)
{
	interconnect_handle_t *h;
	MemoryContext oldContext;

	if (!estate->es_sliceTable)
	{
		elog(ERROR, "SetupInterconnect: no slice table ?");
	}

	Assert(InterconnectContext != NULL);
	oldContext = MemoryContextSwitchTo(InterconnectContext);

	SetupStream(estate);

	MemoryContextSwitchTo(oldContext);

}

/* TeardownInterconnect() function is used to cleanup interconnect resources that
 * were allocated during SetupInterconnect().  This function should ALWAYS be
 * called after SetupInterconnect to avoid leaking resources (like sockets)
 * even if SetupInterconnect did not complete correctly.
 */
void
TeardownInterconnect(EState *estate, bool hasErrors)
{
	TeardownStreams(estate);
}

/*
 * format_sockaddr
 *			Format a sockaddr to a human readable string
 *
 * This function must be kept threadsafe, elog/ereport/palloc etc are not
 * allowed within this function.
 */
char *
format_sockaddr(struct sockaddr_storage *sa, char *buf, size_t len)
{
	int			ret;
	char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];

	ret = pg_getnameinfo_all(sa, sizeof(struct sockaddr_storage),
							 remote_host, sizeof(remote_host),
							 remote_port, sizeof(remote_port),
							 NI_NUMERICHOST | NI_NUMERICSERV);

	if (ret != 0)
		snprintf(buf, len, "?host?:?port?");
	else
	{
#ifdef HAVE_IPV6
		if (sa->ss_family == AF_INET6)
			snprintf(buf, len, "[%s]:%s", remote_host, remote_port);
		else
#endif
			snprintf(buf, len, "%s:%s", remote_host, remote_port);
	}

	return buf;
}

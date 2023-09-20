/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/distributedlog.h"
#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbvars.h"
#include "commands/async.h"
#include "executor/nodeShareInputScan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "postmaster/fts.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "utils/backend_cancel.h"
#include "utils/resource_manager.h"
#include "utils/faultinjector.h"
#include "utils/sharedsnapshot.h"
#include "utils/gpexpand.h"
#include "utils/snapmgr.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbtm.h"
#include "postmaster/backoff.h"
#include "cdb/memquota.h"
#include "executor/instrument.h"
#include "executor/spi.h"
#include "utils/workfile_mgr.h"
#include "utils/session_state.h"
#include "cdb/cdbendpoint.h"
#include "replication/gp_replication.h"

/* GUCs */
int			shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE;

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;
static bool addin_request_allowed = true;


/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.  Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void
RequestAddinShmemSpace(Size size)
{
	if (IsUnderPostmaster || !addin_request_allowed)
		return;					/* too late */
	total_addin_request = add_size(total_addin_request, size);
}


/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 */
void
CreateSharedMemoryAndSemaphores(int port)
{
	PGShmemHeader *shim = NULL;

	if (!IsUnderPostmaster)
	{
		PGShmemHeader *seghdr;
		Size		size;
		int			numSemas;

		/* Compute number of semaphores we'll need */
		numSemas = ProcGlobalSemas();
		numSemas += SpinlockSemas();

        elog(DEBUG3,"reserving %d semaphores",numSemas);
		/*
		 * Size of the Postgres shared-memory block is estimated via
		 * moderately-accurate estimates for the big hogs, plus 100K for the
		 * stuff that's too small to bother with estimating.
		 *
		 * We take some care during this phase to ensure that the total size
		 * request doesn't overflow size_t.  If this gets through, we don't
		 * need to be so careful during the actual allocation phase.
		 */
		size = 150000;
		size = add_size(size, PGSemaphoreShmemSize(numSemas));

		elog(DEBUG1, "1 %lu", (unsigned long) size);

		size = add_size(size, SpinlockSemaSize());
		elog(DEBUG1, "2 %lu", (unsigned long) size);

		size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
												 sizeof(ShmemIndexEnt)));
		elog(DEBUG1, "3 %lu", (unsigned long) size);

		size = add_size(size, BufferShmemSize());
		elog(DEBUG1, "4 %lu", (unsigned long) size);
		//size = add_size(size, LockShmemSize());
		elog(DEBUG1, "5 %lu", (unsigned long) size);
		//size = add_size(size, PredicateLockShmemSize());
		elog(DEBUG1, "6 %lu", (unsigned long) size);

		if (IsResQueueEnabled() && Gp_role == GP_ROLE_DISPATCH)
		{
			size = add_size(size, ResSchedulerShmemSize());
			elog(DEBUG1, "7 %lu", (unsigned long) size);
			size = add_size(size, ResPortalIncrementShmemSize());
			elog(DEBUG1, "8 %lu", (unsigned long) size);
		}
		else if (IsResGroupEnabled()) {
			size = add_size(size, ResGroupShmemSize());
			elog(DEBUG1, "9 %lu", (unsigned long) size);
		}
		size = add_size(size, SharedSnapshotShmemSize());
		elog(DEBUG1, "10 %lu", (unsigned long) size);

		if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY) {
			size = add_size(size, FtsShmemSize());
			elog(DEBUG1, "11 %lu", (unsigned long) size);
		}

		size = add_size(size, ProcGlobalShmemSize());
		elog(DEBUG1, "12 %lu", (unsigned long) size);

		size = add_size(size, XLOGShmemSize());
		elog(DEBUG1, "13 %lu", (unsigned long) size);

		size = add_size(size, DistributedLog_ShmemSize());
		elog(DEBUG1, "14 %lu", (unsigned long) size);

		size = add_size(size, CLOGShmemSize());
		elog(DEBUG1, "15 %lu", (unsigned long) size);

		size = add_size(size, CommitTsShmemSize());
		elog(DEBUG1, "16 %lu", (unsigned long) size);

		size = add_size(size, SUBTRANSShmemSize());
		elog(DEBUG1, "17 %lu", (unsigned long) size);

		size = add_size(size, TwoPhaseShmemSize());
		elog(DEBUG1, "18 %lu", (unsigned long) size);

		size = add_size(size, BackgroundWorkerShmemSize());
		elog(DEBUG1, "19 %lu", (unsigned long) size);

		size = add_size(size, MultiXactShmemSize());
		elog(DEBUG1, "20 %lu", (unsigned long) size);

		size = add_size(size, LWLockShmemSize());
		elog(DEBUG1, "21 %lu", (unsigned long) size);

		size = add_size(size, ProcArrayShmemSize());
		elog(DEBUG1, "22 %lu", (unsigned long) size);

		size = add_size(size, BackendStatusShmemSize());
		elog(DEBUG1, "23 %lu", (unsigned long) size);

		size = add_size(size, SInvalShmemSize());
		elog(DEBUG1, "24 %lu", (unsigned long) size);

		size = add_size(size, PMSignalShmemSize());
		elog(DEBUG1, "25 %lu", (unsigned long) size);

		size = add_size(size, ProcSignalShmemSize());
		elog(DEBUG1, "26 %lu", (unsigned long) size);

		size = add_size(size, CheckpointerShmemSize());
		elog(DEBUG1, "27 %lu", (unsigned long) size);

		size = add_size(size, AutoVacuumShmemSize());
		elog(DEBUG1, "28 %lu", (unsigned long) size);

		size = add_size(size, ReplicationSlotsShmemSize());
		elog(DEBUG1, "29 %lu", (unsigned long) size);

		size = add_size(size, ReplicationOriginShmemSize());
		elog(DEBUG1, "30 %lu", (unsigned long) size);

		size = add_size(size, WalSndShmemSize());
		elog(DEBUG1, "31 %lu", (unsigned long) size);

		size = add_size(size, WalRcvShmemSize());
		elog(DEBUG1, "32 %lu", (unsigned long) size);

		size = add_size(size, ApplyLauncherShmemSize());
		elog(DEBUG1, "33 %lu", (unsigned long) size);

		size = add_size(size, FTSReplicationStatusShmemSize());
		elog(DEBUG1, "34 %lu", (unsigned long) size);

		size = add_size(size, SnapMgrShmemSize());
		elog(DEBUG1, "35 %lu", (unsigned long) size);

		size = add_size(size, BTreeShmemSize());
		elog(DEBUG1, "36 %lu", (unsigned long) size);

		size = add_size(size, SyncScanShmemSize());
		elog(DEBUG1, "37 %lu", (unsigned long) size);

		size = add_size(size, AsyncShmemSize());
		elog(DEBUG1, "38 %lu", (unsigned long) size);

#ifdef EXEC_BACKEND
		size = add_size(size, ShmemBackendArraySize());
#endif

		size = add_size(size, tmShmemSize());
		elog(DEBUG1, "39 %lu", (unsigned long) size);

		size = add_size(size, CheckpointerShmemSize());
		elog(DEBUG1, "40 %lu", (unsigned long) size);

		size = add_size(size, CancelBackendMsgShmemSize());
		elog(DEBUG1, "41 %lu", (unsigned long) size);

		size = add_size(size, WorkFileShmemSize());
		elog(DEBUG1, "42 %lu", (unsigned long) size);

		size = add_size(size, ShareInputShmemSize());
		elog(DEBUG1, "43 %lu", (unsigned long) size);


#ifdef FAULT_INJECTOR
		size = add_size(size, FaultInjector_ShmemSize());
		elog(DEBUG1, "44 %lu", (unsigned long) size);

#endif			

		/* This elog happens before we know the name of the log file we are supposed to use */
		elog(DEBUG1, "Size not including the buffer pool %lu",
			 (unsigned long) size);

		/* freeze the addin request size and include it */
		addin_request_allowed = false;
		size = add_size(size, total_addin_request);
		elog(DEBUG1, "45 %lu", (unsigned long) size);


		/* might as well round it off to a multiple of a typical page size */
		size = add_size(size, BLCKSZ - (size % BLCKSZ));
		elog(DEBUG1, "46 %lu", (unsigned long) size);


		/* Consider the size of the SessionState array */
		size = add_size(size, SessionState_ShmemSize());
		elog(DEBUG1, "47 %lu", (unsigned long) size);


		/* size of Instrumentation slots */
		size = add_size(size, InstrShmemSize());
		elog(DEBUG1, "48 %lu", (unsigned long) size);


		/* size of expand version */
		size = add_size(size, GpExpandVersionShmemSize());
		elog(DEBUG1, "49 %lu", (unsigned long) size);


		/* size of token and endpoint shared memory */
		size = add_size(size, EndpointShmemSize());
		elog(DEBUG1, "50 %lu", (unsigned long) size);


		/* size of parallel cursor count */
		size = add_size(size, ParallelCursorCountSize());
		elog(DEBUG1, "51 %lu", (unsigned long) size);


		elog(DEBUG3, "invoking IpcMemoryCreate(size=%lu)", size);

		/*
		 * Create the shmem segment
		 */
#ifdef SDB_NOUSE
		seghdr = PGSharedMemoryCreate(size, port, &shim);
#endif

		ereport(WARNING,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("ShmemIndex size \"%lu\"",
						size)));
		seghdr = MemoryContextAllocZero(TopMemoryContext, size);
		/* Initialize new segment. */
		//hdr = (PGShmemHeader *) memAddress;
		//seghdr->creatorPID = getpid();
		seghdr->magic = PGShmemMagic;
		seghdr->dsm_control = 0;

		seghdr->totalsize = size;
		seghdr->freeoffset = MAXALIGN(sizeof(PGShmemHeader));

		shim = seghdr;
		/* Save info for possible future use */
		UsedShmemSegAddr = seghdr;

		InitShmemAccess(seghdr);

		/*
		 * Create semaphores
		 */
		PGReserveSemaphores(numSemas, port);

		/*
		 * If spinlocks are disabled, initialize emulation layer (which
		 * depends on semaphores, so the order is important here).
		 */
#ifndef HAVE_SPINLOCKS
		SpinlockSemaInit();
#endif
	}
	else
	{
		/*
		 * We are reattaching to an existing shared memory segment. This
		 * should only be reached in the EXEC_BACKEND case.
		 */
#ifndef EXEC_BACKEND
		elog(PANIC, "should be attached to shared memory already");
#endif
	}

	/*
	 * Set up shared memory allocation mechanism
	 */
	if (!IsUnderPostmaster)
		InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
	CLOGShmemInit();
	DistributedLog_ShmemInit();
	CommitTsShmemInit();
	SUBTRANSShmemInit();
	MultiXactShmemInit();
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
		FtsShmemInit();
	tmShmemInit();
	InitBufferPool();

	/*
	 * Set up lock manager
	 */
	// InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	// InitPredicateLocks();

	/*
	 * Set up resource manager 
	 */
	ResManagerShmemInit();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();

	/* Initialize SessionState shared memory array */
	SessionState_ShmemInit();
	/* Initialize vmem protection */
	GPMemoryProtect_ShmemInit();

	CreateSharedProcArray();
	CreateSharedBackendStatus();
	
	/*
	 * Set up Shared snapshot slots
	 *
	 * TODO: only need to do this if we aren't the QD. for now we are just 
	 *		 doing it all the time and wasting shemem on the QD.  This is 
	 *		 because this happens at postmaster startup time when we don't
	 *		 know who we are.  
	 */
	CreateSharedSnapshotArray();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	ApplyLauncherShmemInit();
	FTSReplicationStatusShmemInit();

#ifdef FAULT_INJECTOR
	FaultInjector_ShmemInit();
#endif

	/*
	 * Set up other modules that need some shared memory space
	 */
	SnapMgrInit();
	BTreeShmemInit();
	SyncScanShmemInit();
	AsyncShmemInit();
	BackendCancelShmemInit();
	WorkFileShmemInit();
	ShareInputShmemInit();

	/*
	 * Set up Instrumentation free list
	 */
	if (!IsUnderPostmaster)
		InstrShmemInit();

	GpExpandVersionShmemInit();

#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 */
	if (!IsUnderPostmaster)
		ShmemBackendArrayAllocation();
#endif

	if (gp_enable_resqueue_priority)
		BackoffStateInit();

	/* Initialize dynamic shared memory facilities. */
	if (!IsUnderPostmaster)
		dsm_postmaster_startup(shim);

	/* Initialize shared memory for parallel retrieve cursor */
	if (!IsUnderPostmaster)
		EndpointShmemInit();
	
	if (Gp_role == GP_ROLE_DISPATCH)
		ParallelCursorCountInit();

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}

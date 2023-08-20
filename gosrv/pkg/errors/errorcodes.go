package errors

const (
	UNKNOWN   = -1
	STATUS_OK = 0

	/* Exec slice table status */
	EXEC_SLICE_CORRUPTION         = 10
	EXEC_SLICE_SEGMENT_EMPTY      = 11
	EXEC_SLICE_CREATE_CONN_FAILED = 12

	/* Query exec status*/
	QUERY_EXEC_NO_STATUS       = 30
	QUERY_EXEC_TIMEOUT         = 31
	QUERY_EXEC_FAILED          = 32
	QUERY_EXEC_RETRY           = 33
	QUERY_EXEC_DESTORY         = 34
	QUERY_EXEC_GANG_CORRUPTION = 35

	/* Bitmapset status */
	BITMAP_NEGATIVE = 60
)

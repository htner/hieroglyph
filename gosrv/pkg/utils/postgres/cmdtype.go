package postgres

const (
	CMD_UNKNOWN = 0
	CMD_SELECT  = 1 /* select stmt */
	CMD_UPDATE  = 2 /* update stmt */
	CMD_INSERT  = 3 /* insert stmt */
	CMD_DELETE  = 4
	CMD_UTILITY = 5 /* cmds like create, destroy, copy, vacuum, etc. */
	CMD_NOTHING = 6 /* dummy command for instead nothing rules with qual */
)



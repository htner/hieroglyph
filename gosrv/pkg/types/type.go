package types

type DatabaseId uint64
type RelId uint64
type TypeId uint32
type KindId uint32

type FileNumber uint64

type SessionId uint64
type TransactionId uint64
type OID uint64
type XState uint8

/*
func GetDbIdByRelId(rel RelId) DatabaseId {
	return DatabaseId(rel >> 32)
}
*/

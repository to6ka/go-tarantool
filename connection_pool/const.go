package connection_pool

type Mode uint32
type Role uint32

// mode
const (
	RW = iota
	PreferRW
	PreferRO
)

// master/replica role
const (
	master = iota
	replica
)

// pool state
const (
	connConnected = iota
	connClosed
)

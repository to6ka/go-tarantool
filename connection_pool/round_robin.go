package connection_pool

import (
	"sync/atomic"

	"github.com/tarantool/go-tarantool"
)

type RoundRobinStrategy struct {
	conns       []*tarantool.Connection
	indexByAddr map[string]int
	size        int
	current     uint64
}

func (r *RoundRobinStrategy) GetConnByAddr(addr string) *tarantool.Connection {
	index, found := r.indexByAddr[addr]
	if !found {
		return nil
	}

	return r.conns[index]
}

func (r *RoundRobinStrategy) DeleteConnByAddr(addr string) *tarantool.Connection {

	if r.size == 0 {
		return nil
	}

	index, found := r.indexByAddr[addr]
	if !found {
		return nil
	}

	delete(r.indexByAddr, addr)

	conn := r.conns[index]
	r.conns = append(r.conns[:index], r.conns[index + 1:]...)
	r.size -= 1

	for index, conn := range r.conns {
		r.indexByAddr[conn.Addr()] = index
	}

	return conn
}

func (r *RoundRobinStrategy) IsEmpty() bool {
	return r.size == 0
}

func (r *RoundRobinStrategy) NextIndex() int {
	return int(atomic.AddUint64(&r.current, uint64(1)) % uint64(len(r.conns)))
}

func (r *RoundRobinStrategy) CloseConns() []error {
	errs := make([]error, len(r.conns))

	for i, conn := range r.conns {
		errs[i] = conn.Close()
	}

	return errs
}

func (r *RoundRobinStrategy) GetNextConnection() *tarantool.Connection {
	next := r.NextIndex()
	cycleLen := len(r.conns) + next
	for i := next; i < cycleLen; i++ {
		idx := i % len(r.conns)
		if r.conns[idx].ConnectedNow() {
			if i != next {
				atomic.StoreUint64(&r.current, uint64(idx))
			}
			return r.conns[idx]
		}
	}
	return nil
}

func NewEmptyRoundRobin(size int) *RoundRobinStrategy {
	return &RoundRobinStrategy{
		conns: make([]*tarantool.Connection, 0, size),
		indexByAddr: make(map[string]int),
		size: 0,
	}
}

func (r *RoundRobinStrategy) AddConn(addr string, conn *tarantool.Connection) {
	r.conns = append(r.conns, conn)
	r.indexByAddr[addr] = r.size
	r.size += 1
}

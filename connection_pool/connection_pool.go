package connection_pool

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tarantool/go-tarantool"
)

var (
	ErrEmptyAddrs        = errors.New("addrs should not be empty")
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
	ErrNoConnection      = errors.New("no active connections")
	ErrTooManyArgs       = errors.New("too many arguments")
	ErrIncorrectResponse = errors.New("Incorrect response format")
	ErrNoRwInstance      = errors.New("Can't find rw instance in pool")
	ErrNoHealthyInstance = errors.New("Can't find healthy instance in pool")
)

type OptsPool struct {
	CheckTimeout         time.Duration
	NodesGetFunctionName string
	ClusterDiscoveryTime time.Duration
}

type ConnectionInfo struct {
	ConnectedNow bool
	ConnRole     Role
}

type ConnectionPool struct {
	addrs    []string
	connOpts tarantool.Opts
	opts     OptsPool

	mutex    sync.RWMutex
	notify   chan tarantool.ConnEvent
	state    uint32
	control  chan struct{}
	roPool   *RoundRobinStrategy
	rwPool   *RoundRobinStrategy
}

// ConnectWithOpts creates pool for instances with addresses `addrs`
// with options `opts`
func ConnectWithOpts(addrs []string, connOpts tarantool.Opts, opts OptsPool) (connPool *ConnectionPool, err error) {
	if len(addrs) == 0 {
		return nil, ErrEmptyAddrs
	}
	if opts.CheckTimeout <= 0 {
		return nil, ErrWrongCheckTimeout
	}
	if opts.ClusterDiscoveryTime <= 0 {
		opts.ClusterDiscoveryTime = 60 * time.Second
	}

	notify := make(chan tarantool.ConnEvent, 10*len(addrs)) // x10 to accept disconnected and closed event (with a margin)
	connOpts.Notify = notify

	size := len(addrs)
	rwPool := NewEmptyRoundRobin(size)
	roPool := NewEmptyRoundRobin(size)

	connPool = &ConnectionPool{
		addrs:    addrs,
		connOpts: connOpts,
		opts:     opts,
		notify:   notify,
		control:  make(chan struct{}),
		rwPool:   rwPool,
		roPool:   roPool,
	}

	somebodyAlive := connPool.fillPools()
	if !somebodyAlive {
		connPool.Close()
		return nil, ErrNoConnection
	}

	go connPool.checker()

	return connPool, nil
}

// ConnectWithOpts creates pool for instances with addresses `addrs`
func Connect(addrs []string, connOpts tarantool.Opts) (connPool *ConnectionPool, err error) {
	opts := OptsPool{
		CheckTimeout: 1 * time.Second,
	}
	return ConnectWithOpts(addrs, connOpts, opts)
}

// ConnectedNow gets connected status of pool
func (connPool *ConnectionPool) ConnectedNow(mode Mode) (bool, error) {
	conn, err := connPool.getCurrentConnection(mode)
	if err != nil || conn == nil {
		return false, err
	}

	return connPool.getState() == connConnected && conn.ConnectedNow(), nil
}

// ConfiguredTimeout gets `timeout` of current connection
func (connPool *ConnectionPool) ConfiguredTimeout(mode Mode) (time.Duration, error) {
	conn, err := connPool.getCurrentConnection(mode)
	if err != nil {
		return 0, err
	}

	return conn.ConfiguredTimeout(), nil
}

// Close closes connections in pool
func (connPool *ConnectionPool) Close() []error {
	connPool.mutex.Lock()
	defer connPool.mutex.Unlock()

	close(connPool.control)
	connPool.state = connClosed

	rwErrs := connPool.rwPool.CloseConns()
	roErrs := connPool.roPool.CloseConns()

	allErrs := append(rwErrs, roErrs...)

	return allErrs
}

// GetAddrs gets addresses of connections in Pool
func (connPool *ConnectionPool) GetAddrs() []string {
	return connPool.addrs
}

// GetNodesGetFunctionName gets name of function for getting names of nodes
func (connPool *ConnectionPool) GetNodesGetFunctionName() string {
	return connPool.opts.NodesGetFunctionName
}

// GetPoolInfo gets information of connections (connected status, ro/rw role)
func (connPool *ConnectionPool) GetPoolInfo() map[string]*ConnectionInfo {
	info := make(map[string]*ConnectionInfo)

	for _, addr := range connPool.addrs {
		conn, role := connPool.getConnectionFromPool(addr)
		if conn != nil {
			info[addr] = &ConnectionInfo{ConnectedNow: conn.ConnectedNow(), ConnRole: role}
		}
	}

	return info
}

// Ping sends empty request to Tarantool to check connection.
func (connPool *ConnectionPool) Ping(userMode ...Mode) (*tarantool.Response, error) {
	conn, err := connPool.getConnByMode(PreferRO, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Ping()
}

// Select performs select to box space.
func (connPool *ConnectionPool) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(PreferRO, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Select(space, index, offset, limit, iterator, key)
}

// Insert performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connPool *ConnectionPool) Insert(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil, err
	}

	return conn.Insert(space, tuple)
}

// Replace performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
func (connPool *ConnectionPool) Replace(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil, err
	}

	return conn.Replace(space, tuple)
}

// Delete performs deletion of a tuple by key.
// Result will contain array with deleted tuple.
func (connPool *ConnectionPool) Delete(space, index interface{}, key interface{}) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil, err
	}

	return conn.Delete(space, index, key)
}

// Update performs update of a tuple by key.
// Result will contain array with updated tuple.
func (connPool *ConnectionPool) Update(space, index interface{}, key, ops interface{}) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil, err
	}

	return conn.Update(space, index, key, ops)
}

// Upsert performs "update or insert" action of a tuple by key.
// Result will not contain any tuple.
func (connPool *ConnectionPool) Upsert(space interface{}, tuple, ops interface{}) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil, err
	}

	return conn.Upsert(space, tuple, ops)
}

// Call calls registered tarantool function.
// It uses request code for tarantool 1.6, so result is converted to array of arrays
func (connPool *ConnectionPool) Call(functionName string, args interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call(functionName, args)
}

// Call17 calls registered tarantool function.
// It uses request code for tarantool 1.7, so result is not converted
// (though, keep in mind, result is always array)
func (connPool *ConnectionPool) Call17(functionName string, args interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call17(functionName, args)
}

// Eval passes lua expression for evaluation.
func (connPool *ConnectionPool) Eval(expr string, args interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Eval(expr, args)
}

// GetTyped performs select (with limit = 1 and offset = 0)
// to box space and fills typed result.
func (connPool *ConnectionPool) GetTyped(space, index interface{}, key interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(PreferRO, userMode)
	if err != nil {
		return err
	}

	return conn.GetTyped(space, index, key, result)
}

// SelectTyped performs select to box space and fills typed result.
func (connPool *ConnectionPool) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(PreferRO, userMode)
	if err != nil {
		return err
	}

	return conn.SelectTyped(space, index, offset, limit, iterator, key, result)
}

// InsertTyped performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connPool *ConnectionPool) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return err
	}

	return conn.InsertTyped(space, tuple, result)
}

// ReplaceTyped performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
func (connPool *ConnectionPool) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return err
	}

	return conn.ReplaceTyped(space, tuple, result)
}

// DeleteTyped performs deletion of a tuple by key and fills result with deleted tuple.
func (connPool *ConnectionPool) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return err
	}

	return conn.DeleteTyped(space, index, key, result)
}

// UpdateTyped performs update of a tuple by key and fills result with updated tuple.
func (connPool *ConnectionPool) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return err
	}

	return conn.UpdateTyped(space, index, key, ops, result)
}

// CallTyped calls registered function.
// It uses request code for tarantool 1.6, so result is converted to array of arrays
func (connPool *ConnectionPool) CallTyped(functionName string, args interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.CallTyped(functionName, args, result)
}

// Call17Typed calls registered function.
// It uses request code for tarantool 1.7, so result is not converted
// (though, keep in mind, result is always array)
func (connPool *ConnectionPool) Call17Typed(functionName string, args interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.Call17Typed(functionName, args, result)
}

// EvalTyped passes lua expression for evaluation.
func (connPool *ConnectionPool) EvalTyped(expr string, args interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.EvalTyped(expr, args, result)
}

// SelectAsync sends select request to tarantool and returns Future.
func (connPool *ConnectionPool) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(PreferRO, userMode)
	if err != nil {
		return nil
	}

	return conn.SelectAsync(space, index, offset, limit, iterator, key)
}

// InsertAsync sends insert action to tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connPool *ConnectionPool) InsertAsync(space interface{}, tuple interface{}) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil
	}

	return conn.InsertAsync(space, tuple)
}

// ReplaceAsync sends "insert or replace" action to tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
func (connPool *ConnectionPool) ReplaceAsync(space interface{}, tuple interface{}) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil
	}

	return conn.ReplaceAsync(space, tuple)
}

// DeleteAsync sends deletion action to tarantool and returns Future.
// Future's result will contain array with deleted tuple.
func (connPool *ConnectionPool) DeleteAsync(space, index interface{}, key interface{}) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil
	}

	return conn.DeleteAsync(space, index, key)
}

// UpdateAsync sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
func (connPool *ConnectionPool) UpdateAsync(space, index interface{}, key, ops interface{}) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil
	}

	return conn.UpdateAsync(space, index, key, ops)
}

// UpsertAsync sends "update or insert" action to tarantool and returns Future.
// Future's sesult will not contain any tuple.
func (connPool *ConnectionPool) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, nil)
	if err != nil {
		return nil
	}

	return conn.UpsertAsync(space, tuple, ops)
}

// CallAsync sends a call to registered tarantool function and returns Future.
// It uses request code for tarantool 1.6, so future's result is always array of arrays
func (connPool *ConnectionPool) CallAsync(functionName string, args interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil
	}

	return conn.CallAsync(functionName, args)
}

// Call17Async sends a call to registered tarantool function and returns Future.
// It uses request code for tarantool 1.7, so future's result will not be converted
// (though, keep in mind, result is always array)
func (connPool *ConnectionPool) Call17Async(functionName string, args interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil
	}

	return conn.Call17Async(functionName, args)
}

// EvalAsync sends a lua expression for evaluation and returns Future.
func (connPool *ConnectionPool) EvalAsync(expr string, args interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil
	}

	return conn.EvalAsync(expr, args)
}

//
// private
//

func (connPool *ConnectionPool) getConnectionRole(conn *tarantool.Connection) (Role, error) {
	resp, err := conn.Call17("box.info", []interface{}{})
	if err != nil {
		return replica, err
	}
	if resp == nil {
		return replica, ErrIncorrectResponse
	}
	if len(resp.Data) < 1 {
		return replica, ErrIncorrectResponse
	}

	replicaRole := resp.Data[0].(map[interface{}]interface{})["ro"]
	switch replicaRole {
	case false:
		return master, nil
	case true:
		return replica, nil
	}

	return replica, nil
}

func (connPool *ConnectionPool) getConnectionFromPool(addr string) (*tarantool.Connection, Role) {
	connPool.mutex.RLock()
	defer connPool.mutex.RUnlock()

	conn := connPool.rwPool.GetConnByAddr(addr)
	if conn != nil {
		return conn, master
	}

	return connPool.roPool.GetConnByAddr(addr), replica
}

func (connPool *ConnectionPool) deleteConnectionFromPool(addr string) {
	connPool.mutex.Lock()
	defer connPool.mutex.Unlock()

	conn := connPool.rwPool.DeleteConnByAddr(addr)
	if conn != nil {
		return
	}

	connPool.roPool.DeleteConnByAddr(addr)
}

func (connPool *ConnectionPool) setConnectionToPool(addr string, conn *tarantool.Connection) error {
	connPool.mutex.Lock()
	defer connPool.mutex.Unlock()

	role, err := connPool.getConnectionRole(conn)
	if err != nil {
		return err
	}

	switch role {
	case master:
		connPool.rwPool.AddConn(addr, conn)
	case replica:
		connPool.roPool.AddConn(addr, conn)
	}

	return nil
}

func (connPool *ConnectionPool) isAddrIncluded(addr string, addrs []string) bool {
	for _, v := range addrs {
		if addr == v {
			return true
		}
	}
	return false
}

func (connPool *ConnectionPool) addNewConnections(addrs []string) {
	for _, addr := range addrs {
		if !connPool.isAddrIncluded(addr, connPool.addrs) {
			conn, _ := tarantool.Connect(addr, connPool.connOpts)
			if conn != nil {
				connPool.setConnectionToPool(addr, conn)
			}
		}
	}
}

func (connPool *ConnectionPool) deleteObsoleteConnections(addrs []string) {
	for _, addr := range connPool.addrs {
		if !connPool.isAddrIncluded(addr, addrs) {
			conn, _ := connPool.getConnectionFromPool(addr)
			if conn != nil {
				conn.Close()
			}
			connPool.deleteConnectionFromPool(addr)
		}
	}
}

func (connPool *ConnectionPool) refreshConnection(addr string) {
	if conn, oldRole := connPool.getConnectionFromPool(addr); conn != nil {
		if !conn.ClosedNow() {
			curRole, _ := connPool.getConnectionRole(conn)
			if oldRole != curRole {
				connPool.deleteConnectionFromPool(addr)
				connPool.setConnectionToPool(addr, conn)
			}
		}
	} else {
		conn, _ := tarantool.Connect(addr, connPool.connOpts)
		if conn != nil {
			connPool.setConnectionToPool(addr, conn)
		}
	}
}

func (connPool *ConnectionPool) checker() {

	refreshTimer := time.NewTicker(connPool.opts.ClusterDiscoveryTime)
	timer := time.NewTicker(connPool.opts.CheckTimeout)
	defer refreshTimer.Stop()
	defer timer.Stop()

	for connPool.getState() != connClosed {
		select {
		case <-connPool.control:
			return
		case e := <-connPool.notify:
			if connPool.getState() == connClosed {
				return
			}
			if e.Conn.ClosedNow() {
				addr := e.Conn.Addr()
				if conn, _ := connPool.getConnectionFromPool(addr); conn == nil {
					continue
				}
				conn, _ := tarantool.Connect(addr, connPool.connOpts)
				if conn != nil {
					connPool.setConnectionToPool(addr, conn)
				} else {
					connPool.deleteConnectionFromPool(addr)
				}
			}
		case <-refreshTimer.C:
			if connPool.getState() == connClosed || connPool.opts.NodesGetFunctionName == "" {
				continue
			}
			var resp [][]string
			err := connPool.Call17Typed(connPool.opts.NodesGetFunctionName, []interface{}{}, &resp)
			if err != nil {
				continue
			}
			if len(resp) > 0 && len(resp[0]) > 0 {
				addrs := resp[0]
				// Fill pool with new connections
				connPool.addNewConnections(addrs)

				// Clear pool from obsolete connections
				connPool.deleteObsoleteConnections(addrs)

				connPool.addrs = addrs
			}
		case <-timer.C:
			for _, addr := range connPool.addrs {
				if connPool.getState() == connClosed {
					return
				}

				// Reopen connection
				// Relocate connection between subpools
				// if ro/rw was updated
				connPool.refreshConnection(addr)
			}
		}
	}
}

func (connPool *ConnectionPool) fillPools() bool {
	somebodyAlive := false

	for _, addr := range connPool.addrs {
		conn, err := tarantool.Connect(addr, connPool.connOpts)
		if err != nil {
			log.Printf("tarantool: connect to %s failed: %s\n", addr, err.Error())
		} else if conn != nil {
			err = connPool.setConnectionToPool(addr, conn)
			if err != nil {
				conn.Close()
				log.Printf("tarantool: storing connection to %s failed: %s\n", addr, err.Error())
			} else if conn.ConnectedNow() {
				somebodyAlive = true
			}
		}
	}

	return somebodyAlive
}

func (connPool *ConnectionPool) getState() uint32 {
	return atomic.LoadUint32(&connPool.state)
}

func (connPool *ConnectionPool) getCurrentConnection(mode Mode) (*tarantool.Connection, error) {
	connPool.mutex.RLock()
	defer connPool.mutex.RUnlock()

	switch mode {
	case RW:
		if connPool.rwPool.IsEmpty() {
			return nil, ErrNoRwInstance
		}

		return connPool.rwPool.GetNextConnection(), nil

	case PreferRW:
		if !connPool.rwPool.IsEmpty() {
			return connPool.rwPool.GetNextConnection(), nil
		}

		if !connPool.roPool.IsEmpty() {
			return connPool.roPool.GetNextConnection(), nil
		}

		return nil, ErrNoHealthyInstance

	case PreferRO:
		if !connPool.roPool.IsEmpty() {
			return connPool.roPool.GetNextConnection(), nil
		}

		if !connPool.rwPool.IsEmpty() {
			return connPool.rwPool.GetNextConnection(), nil
		}

		return nil, ErrNoHealthyInstance
	}

	return nil, ErrNoHealthyInstance
}

func (connPool *ConnectionPool) getConnByMode(defaultMode Mode, userMode []Mode) (*tarantool.Connection, error) {
	if len(userMode) > 1 {
		return nil, ErrTooManyArgs
	}

	mode := defaultMode
	if len(userMode) > 0 {
		mode = userMode[0]
	}

	return connPool.getCurrentConnection(mode)
}

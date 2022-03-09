package connection_pool_test

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/connection_pool"
	"github.com/tarantool/go-tarantool/test_helpers"
)

var spaceNo = uint32(520)
var spaceName = "testPool"
var indexNo = uint32(0)
var indexName = "pk"

var ports   = []string{"3013", "3014", "3015",
                       "3016", "3017"}
var host    = "127.0.0.1"
var servers = []string{strings.Join([]string{host, ports[0]}, ":"),
                       strings.Join([]string{host, ports[1]}, ":"),
                       strings.Join([]string{host, ports[2]}, ":"),
                       strings.Join([]string{host, ports[3]}, ":"),
                       strings.Join([]string{host, ports[4]}, ":")}

var connOpts = tarantool.Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var connOptsPool = connection_pool.OptsPool{
	CheckTimeout:         1 * time.Second,
	NodesGetFunctionName: "get_cluster_nodes",
	ClusterDiscoveryTime: 3 * time.Second,
}

var instances []test_helpers.TarantoolInstance

func TestConnError_IncorrectParams(t *testing.T) {
	connPool, err := connection_pool.Connect([]string{}, tarantool.Opts{})
	if err == nil {
		t.Errorf("err is nil with incorrect params")
	}
	if connPool != nil {
		t.Errorf("conn is not nil with incorrect params")
	}
	if err.Error() != "addrs should not be empty" {
		t.Errorf("incorrect error: %s", err.Error())
	}

	connPool, err = connection_pool.Connect([]string{"err1", "err2"}, connOpts)
	if err == nil {
		t.Errorf("err is nil with incorrect params")
		return
	}
	if connPool != nil {
		t.Errorf("conn is not nil with incorrect params")
		return
	}

	connPool, err = connection_pool.ConnectWithOpts(servers, tarantool.Opts{}, connection_pool.OptsPool{})
	if err == nil {
		t.Errorf("err is nil with incorrect params")
	}
	if connPool != nil {
		t.Errorf("conn is not nil with incorrect params")
	}
	if err.Error() != "wrong check timeout, must be greater than 0" {
		t.Errorf("incorrect error: %s", err.Error())
	}
}

func TestRefresh(t *testing.T) {
	connPool, _ := connection_pool.ConnectWithOpts(servers, connOpts, connOptsPool)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	curAddr := connPool.GetAddrs()[0]

	// wait for refresh timer
	// scenario 1 nodeload, 1 refresh, 1 nodeload
	timer := time.NewTimer(10 * time.Second)
	<-timer.C

	newAddr := connPool.GetAddrs()[0]

	if curAddr == newAddr {
		t.Errorf("Expect address refresh")
	}

	if connected, _ := connPool.ConnectedNow(connection_pool.PreferRO); !connected {
		t.Errorf("Expect connection to exist")
	}

	_, err := connPool.Call(connPool.GetNodesGetFunctionName(), []interface{}{})
	if err != nil {
		t.Error("Expect to get data after reconnect")
	}
}

func TestConnSuccessfully(t *testing.T) {
	server := servers[0]
	connPool, err := connection_pool.Connect([]string{"err", server}, connOpts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer connPool.Close()

	if connected, _ := connPool.ConnectedNow(connection_pool.PreferRO); !connected {
		t.Errorf("conn has incorrect status")
		return
	}

	poolInfo := connPool.GetPoolInfo()
	if poolInfo[server] == nil || !poolInfo[server].ConnectedNow {
		t.Errorf("incorrect conn status after reconnecting")
	}
}

func TestReconnect(t *testing.T) {
	server := servers[0]

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	test_helpers.StopTarantoolWithCleanup(instances[0])

	timer = time.NewTimer(300 * time.Millisecond)
	<-timer.C

	poolInfo := connPool.GetPoolInfo()
	if poolInfo[server] != nil && poolInfo[server].ConnectedNow {
		t.Errorf("incorrect conn status after disconnect")
	}
	if connected, _ := connPool.ConnectedNow(connection_pool.PreferRO); !connected {
		t.Errorf("incorrect connPool status after reconnecting")
	}

	test_helpers.RestartTarantool(&instances[0])

	timer = time.NewTimer(10 * time.Second)
	<-timer.C

	poolInfo = connPool.GetPoolInfo()
	if poolInfo[server] == nil || !poolInfo[server].ConnectedNow {
		t.Errorf("incorrect conn status after reconnecting")
	}
}

func TestDisconnectAll(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, _ := connection_pool.Connect([]string{server1, server2}, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	test_helpers.StopTarantoolWithCleanup(instances[0])
	test_helpers.StopTarantoolWithCleanup(instances[1])

	timer = time.NewTimer(300 * time.Millisecond)
	<-timer.C

	if connected, _ := connPool.ConnectedNow(connection_pool.PreferRO); connected {
		t.Errorf("incorrect status after disconnect all")
	}

	poolInfo := connPool.GetPoolInfo()
	if poolInfo[server1] != nil && poolInfo[server1].ConnectedNow {
		t.Errorf("incorrect server1 conn status after disconnect")
	}

	if poolInfo[server2] != nil && poolInfo[server2].ConnectedNow {
		t.Errorf("incorrect server2 conn status after disconnect")
	}

	test_helpers.RestartTarantool(&instances[0])
	test_helpers.RestartTarantool(&instances[1])

	timer = time.NewTimer(10 * time.Second)
	<-timer.C

	if connected, _ := connPool.ConnectedNow(connection_pool.PreferRO); !connected {
		t.Errorf("incorrect connPool status after reconnecting")
	}

	poolInfo = connPool.GetPoolInfo()
	if poolInfo[server1] == nil || !poolInfo[server1].ConnectedNow {
		t.Errorf("incorrect server1 conn status after reconnecting")
	}

	if poolInfo[server2] == nil || !poolInfo[server2].ConnectedNow {
		t.Errorf("incorrect server2 conn status after reconnecting")
	}
}

func TestClose(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, _ := connection_pool.Connect([]string{server1, server2}, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C

	poolInfo := connPool.GetPoolInfo()
	if poolInfo[server1] == nil || !poolInfo[server1].ConnectedNow {
		t.Errorf("incorrect conn server1 status")
	}

	if poolInfo[server2] == nil || !poolInfo[server2].ConnectedNow {
		t.Errorf("incorrect conn server2 status")
	}

	connPool.Close()
	timer = time.NewTimer(100 * time.Millisecond)
	<-timer.C

	if connected, _ := connPool.ConnectedNow(connection_pool.PreferRO); connected {
		t.Errorf("incorrect connPool status after close")
	}
	poolInfo = connPool.GetPoolInfo()
	if poolInfo[server1] == nil || poolInfo[server1].ConnectedNow {
		t.Errorf("incorrect server1 conn status after close")
	}

	if poolInfo[server2] == nil || poolInfo[server2].ConnectedNow {
		t.Errorf("incorrect server2 conn status after close")
	}
}

func TestCall(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	resp, err := connPool.Call17("box.info", []interface{}{}, connection_pool.PreferRO)
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Call")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Call")
	}
	val := resp.Data[0].(map[interface{}]interface{})["ro"]
	if val != true {
		t.Errorf("Mode `PreferRO`: expected `true`, but got %v", val)
	}

	resp, err = connPool.Call17("box.info", []interface{}{}, connection_pool.PreferRW)
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Call")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Call")
	}
	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	if val != false {
		t.Errorf("Mode `PreferRW`: expected `false`, but got %v", val)
	}

	resp, err = connPool.Call17("box.info", []interface{}{}, connection_pool.RW)
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Call")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Call")
	}
	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	if val != false {
		t.Errorf("Mode `RW`: expected `false`, but got %v", val)
	}
}

func TestEval(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	resp, err := connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.PreferRO)
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val := resp.Data[0].(bool)
	if val != true {
		t.Errorf("Mode `PreferRO`: expected `true`, but got %v", val)
	}

	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.PreferRW)
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val = resp.Data[0].(bool)
	if val != false {
		t.Errorf("Mode `PreferRW`: expected `false`, but got %v", val)
	}

	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.RW)
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val = resp.Data[0].(bool)
	if val != false {
		t.Errorf("Mode `RW`: expected `false`, but got %v", val)
	}
}

func TestRoundRobinStrategy(t *testing.T) {
	roles := []bool{false, true, false, false, true}
	masterPorts := []string{servers[0], servers[2], servers[3]}
	replicaPorts := []string{servers[1], servers[4]}
	serversNumber := len(servers)

	var rwPorts []string
	var preferRoPorts []string
	var preferRwPorts []string

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		rwPorts = append(rwPorts, resp.Data[0].(string))
	}
	assert.Subset(t, rwPorts, masterPorts)
	assert.Subset(t, masterPorts, rwPorts)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRwPorts = append(preferRwPorts, resp.Data[0].(string))
	}
	assert.Subset(t, preferRwPorts, masterPorts)
	assert.Subset(t, masterPorts, preferRwPorts)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRO)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRoPorts = append(preferRoPorts, resp.Data[0].(string))
	}
	assert.Subset(t, preferRoPorts, replicaPorts)
	assert.Subset(t, replicaPorts, preferRoPorts)
}

func TestRoundRobinStrategy_NoReplica(t *testing.T) {
	roles := []bool{false, false, false, false, false}
	serversNumber := len(servers)

	var rwPorts []string
	var preferRoPorts []string
	var preferRwPorts []string

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		rwPorts = append(rwPorts, resp.Data[0].(string))
	}
	assert.ElementsMatch(t, rwPorts, servers)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRwPorts = append(preferRwPorts, resp.Data[0].(string))
	}
	assert.ElementsMatch(t, preferRwPorts, servers)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRO)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRoPorts = append(preferRoPorts, resp.Data[0].(string))
	}
	assert.ElementsMatch(t, preferRoPorts, servers)
}

func TestRoundRobinStrategy_NoMaster(t *testing.T) {
	roles := []bool{true, true, true, true, true}
	serversNumber := len(servers)

	var preferRoPorts []string
	var preferRwPorts []string

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	_, err = connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RW)
	if err == nil {
		t.Errorf("Expected to fail after Eval, but error is nil")
	}
	if err.Error() != "Can't find rw instance in pool" {
		t.Errorf("Failed with unexpected error %s", err.Error())
	}

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRwPorts = append(preferRwPorts, resp.Data[0].(string))
	}
	assert.ElementsMatch(t, preferRwPorts, servers)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRO)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRoPorts = append(preferRoPorts, resp.Data[0].(string))
	}
	assert.ElementsMatch(t, preferRoPorts, servers)
}

func TestUpdateInstancesRoles(t *testing.T) {
	roles := []bool{false, true, false, false, true}
	masterPorts := []string{servers[0], servers[2], servers[3]}
	replicaPorts := []string{servers[1], servers[4]}
	serversNumber := len(servers)

	var rwPorts []string
	var preferRoPorts []string
	var preferRwPorts []string

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		rwPorts = append(rwPorts, resp.Data[0].(string))
	}
	assert.Subset(t, rwPorts, masterPorts)
	assert.Subset(t, masterPorts, rwPorts)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRwPorts = append(preferRwPorts, resp.Data[0].(string))
	}
	assert.Subset(t, preferRwPorts, masterPorts)
	assert.Subset(t, masterPorts, preferRwPorts)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRO)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRoPorts = append(preferRoPorts, resp.Data[0].(string))
	}
	assert.Subset(t, preferRoPorts, replicaPorts)
	assert.Subset(t, replicaPorts, preferRoPorts)

	roles = []bool{true, false, true, true, false}
	masterPorts = []string{servers[1], servers[4]}
	replicaPorts = []string{servers[0], servers[2], servers[3]}
	serversNumber = len(servers)

	rwPorts = []string{}
	preferRoPorts = []string{}
	preferRwPorts = []string{}

	err = test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	timer = time.NewTimer(10 * time.Second)
	<-timer.C

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		rwPorts = append(rwPorts, resp.Data[0].(string))
	}
	assert.Subset(t, rwPorts, masterPorts)
	assert.Subset(t, masterPorts, rwPorts)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRW)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRwPorts = append(preferRwPorts, resp.Data[0].(string))
	}
	assert.Subset(t, preferRwPorts, masterPorts)
	assert.Subset(t, masterPorts, preferRwPorts)

	for i := 0; i < serversNumber; i++ {
		resp, err := connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.PreferRO)
		if err != nil {
			t.Errorf("Failed to Eval: %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			t.Errorf("Response.Data is empty after Eval")
		}
		preferRoPorts = append(preferRoPorts, resp.Data[0].(string))
	}
	assert.Subset(t, preferRoPorts, replicaPorts)
	assert.Subset(t, replicaPorts, preferRoPorts)
}

func TestInsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Insert(spaceName, []interface{}{"insert_key", "insert_value"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Insert (tuple len)")
			return
		}
		if key, ok := tpl[0].(string); !ok || key != "insert_key" {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "insert_value" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil {
		t.Errorf("Fail to connect to %s: %s", servers[2], err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"insert_key"})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "insert_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "insert_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}
}

func TestDelete(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil {
		t.Errorf("Fail to connect to %s: %s", servers[2], err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	resp, err := conn.Insert(spaceNo, []interface{}{"delete_key", "delete_value"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Insert (tuple len)")
			return
		}
		if key, ok := tpl[0].(string); !ok || key != "delete_key" {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "delete_value" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Delete(spaceName, indexNo, []interface{}{"delete_key"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Insert (tuple len)")
			return
		}
		if key, ok := tpl[0].(string); !ok || key != "delete_key" {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "delete_value" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}
}

func TestUpsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil {
		t.Errorf("Fail to connect to %s: %s", servers[2], err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Upsert(spaceName, []interface{}{"upsert_key", "upsert_value"}, []interface{}{[]interface{}{"=", 1, "new_value"}})
	if err != nil {
		t.Errorf("Failed to Upsert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Upsert")
		return
	}

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"upsert_key"})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "upsert_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "upsert_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Upsert(spaceName, []interface{}{"upsert_key", "upsert_value"}, []interface{}{[]interface{}{"=", 1, "new_value"}})
	if err != nil {
		t.Errorf("Failed to Upsert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Upsert")
		return
	}

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"upsert_key"})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "upsert_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "new_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}
}

func TestUpdate(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil {
		t.Errorf("Fail to connect to %s: %s", servers[2], err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	resp, err := conn.Insert(spaceNo, []interface{}{"update_key", "update_value"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Insert (tuple len)")
			return
		}
		if key, ok := tpl[0].(string); !ok || key != "update_key" {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "update_value" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Update(spaceName, indexNo, []interface{}{"update_key"}, []interface{}{[]interface{}{"=", 1, "new_value"}})
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
		return
	}

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"update_key"})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "update_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "new_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}
}

func TestReplace(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil {
		t.Errorf("Fail to connect to %s: %s", servers[2], err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	resp, err := conn.Insert(spaceNo, []interface{}{"replace_key", "replace_value"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Insert (tuple len)")
			return
		}
		if key, ok := tpl[0].(string); !ok || key != "replace_key" {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "replace_value" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Replace(spaceNo, []interface{}{"new_key", "new_value"})
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
		return
	}

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"new_key"})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "new_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "new_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}
}

func TestSelect(t *testing.T) {
	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	roles := []bool{true, true, false, true, false}

	roServers := []string{servers[0], servers[1], servers[3]}
	rwServers := []string{servers[2], servers[4]}

	roTpl := []interface{}{"ro_select_key", "ro_select_value"}
	rwTpl := []interface{}{"rw_select_key", "rw_select_value"}

	roKey := []interface{}{"ro_select_key"}
	rwKey := []interface{}{"rw_select_key"}

	err := test_helpers.InsertOnInstances(roServers, connOpts, spaceNo, roTpl)
	if err != nil {
		t.Errorf("%s", err.Error())
	}

	err = test_helpers.InsertOnInstances(rwServers, connOpts, spaceNo, rwTpl)
	if err != nil {
		t.Errorf("%s", err.Error())
	}

	err = test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	resp, err := connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey)
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "ro_select_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "ro_select_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, connection_pool.PreferRO)
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "ro_select_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "ro_select_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, connection_pool.PreferRW)
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "rw_select_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "rw_select_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, connection_pool.RW)
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
		return
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if key, ok := tpl[0].(string); !ok || key != "rw_select_key" {
			t.Errorf("Unexpected body of Select (0)")
		}
		if value, ok := tpl[1].(string); !ok || value != "rw_select_value" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}
}

func TestPing(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		t.Errorf("fail to set roles for cluster: %s", err.Error())
		return
	}

	connPool, _ := connection_pool.Connect(servers, connOpts)
	if connPool == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer connPool.Close()

	resp, err := connPool.Ping()
	if err != nil {
		t.Errorf("Failed to Ping: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Ping")
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	initScript := "config.lua"
	waitStart := 100 * time.Millisecond
	var connectRetry uint = 3
	retryTimeout := 500 * time.Millisecond
	workDirs := []string{"work_dir1", "work_dir2",
		                 "work_dir3", "work_dir4",
		                 "work_dir5"}
	var err error

	instances, err = test_helpers.StartTarantoolInstances(servers, workDirs, test_helpers.StartOpts{
		InitScript:   initScript,
		User:         connOpts.User,
		Pass:         connOpts.Pass,
		WaitStart:    waitStart,
		ConnectRetry: connectRetry,
		RetryTimeout: retryTimeout,
	})

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
		return -1
	}

	defer test_helpers.StopTarantoolInstances(instances)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}

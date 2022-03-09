package test_helpers

import (
	"fmt"

	"github.com/tarantool/go-tarantool"
)

func compareTuples(expectedTpl []interface{}, actualTpl []interface{}) error {
	if len(actualTpl) != len(expectedTpl) {
		return fmt.Errorf("Unexpected body of Insert (tuple len)")
	}

	for i, field := range actualTpl {
		if field != expectedTpl[i] {
			return fmt.Errorf("Unexpected field, expected: %v actual: %v", expectedTpl[i], field)
		}
	}

	return nil
}

func InsertOnInstance(server string, connOpts tarantool.Opts, space interface{}, tuple interface{}) error {
	conn, err := tarantool.Connect(server, connOpts)
	if err != nil {
		return fmt.Errorf("Fail to connect to %s: %s", server, err.Error())
	}
	if conn == nil {
		return fmt.Errorf("conn is nil after Connect")
	}
	defer conn.Close()

	resp, err := conn.Insert(space, tuple)
	if err != nil {
		return fmt.Errorf("Failed to Insert: %s", err.Error())
	}
	if resp == nil {
		return fmt.Errorf("Response is nil after Insert")
	}
	if len(resp.Data) != 1 {
		return fmt.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		return fmt.Errorf("Unexpected body of Insert")
	} else {
		expectedTpl, ok := tuple.([]interface{})
		if !ok {
			return fmt.Errorf("Failed to cast")
		}

		err = compareTuples(expectedTpl, tpl)
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertOnInstances(servers []string, connOpts tarantool.Opts, space interface{}, tuple interface{}) error {
	serversNumber := len(servers)
	roles := make([]bool, serversNumber)
	for i:= 0; i < serversNumber; i++{
		roles[i] = false
	}

	err := SetClusterRoles(servers, connOpts, roles)
	if err != nil {
		return fmt.Errorf("fail to set roles for cluster: %s", err.Error())
	}

	for _, server := range servers {
		err := InsertOnInstance(server, connOpts, space, tuple)
		if err != nil {
			return err
		}
	}

	return nil
}

func SetInstanceRole(server string, connOpts tarantool.Opts, isReplica bool) error {
	conn, err := tarantool.Connect(server, connOpts)
	defer conn.Close()

	if err != nil {
		return err
	}

	_, err = conn.Call("box.cfg", []interface{}{map[string]bool{"read_only": isReplica}})
	if err != nil {
		return err
	}

	return nil
}

func SetClusterRoles(servers []string, connOpts tarantool.Opts, roles []bool) error {
	if len(servers) != len(roles) {
		return fmt.Errorf("number of servers should be equal to number of roles")
	}

	for i, server := range servers {
		err := SetInstanceRole(server, connOpts, roles[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func StartTarantoolInstances(servers []string, workDirs []string, opts StartOpts) ([]TarantoolInstance, error) {
	if len(servers) != len(workDirs) {
		return nil, fmt.Errorf("number of servers should be equal to number of workDirs")
	}

	instances := make([]TarantoolInstance, 0, len(servers))

	for i, server := range servers {
		opts.Listen = server
		opts.WorkDir = workDirs[i]

		instance, err := StartTarantool(opts)
		if err != nil {
			StopTarantoolInstances(instances)
			return nil, err
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

func StopTarantoolInstances(instances []TarantoolInstance) {
	for _, instance := range instances {
		StopTarantoolWithCleanup(instance)
	}
}

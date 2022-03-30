package tarantool_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
)

type Tuple struct {
	/* instruct msgpack to pack this struct as array,
	 * so no custom packer is needed */
	_msgpack struct{} `msgpack:",asArray"`
	Id       uint
	Msg      string
	Name     string
}

func example_connect() (*tarantool.Connection, error) {
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		return nil, err
	}
	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	_, err = conn.Replace(spaceNo, []interface{}{uint(1112), "hallo", "werld"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func ExampleConnection_Select() {
	var conn *tarantool.Connection
	conn, err := example_connect()
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer conn.Close()
	resp, err := conn.Select(512, 0, 0, 100, tarantool.IterEq, []interface{}{uint(1111)})
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	resp, err = conn.Select("test", "primary", 0, 100, tarantool.IterEq, tarantool.IntKey{1111})
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	// Output:
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
}

func ExampleConnection_SelectTyped() {
	var conn *tarantool.Connection
	conn, err := example_connect()
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer conn.Close()
	var res []Tuple
	err = conn.SelectTyped(512, 0, 0, 100, tarantool.IterEq, tarantool.IntKey{1111}, &res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	err = conn.SelectTyped("test", "primary", 0, 100, tarantool.IterEq, tarantool.IntKey{1111}, &res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	// Output:
	// response is [{{} 1111 hello world}]
	// response is [{{} 1111 hello world}]
}

// Example demonstrates how to use data operations.
func Example_dataOperations() {
	spaceNo := uint32(512)
	indexNo := uint32(0)

	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       50 * time.Millisecond,
		Reconnect:     100 * time.Millisecond,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	client, err := tarantool.Connect(server, opts)
	if err != nil {
		fmt.Errorf("Failed to connect: %s", err.Error())
		return
	}

	// Ping a Tarantool instance to check connection
	resp, err := client.Ping()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)

	// Delete tuples with primary key { 10 } and { 11 }
	client.Delete(spaceNo, indexNo, []interface{}{uint(10)})
	// the same:
	client.Delete("test", "primary", []interface{}{uint(11)})

	// Insert a new tuple { 10, 1 }
	resp, err = client.Insert(spaceNo, []interface{}{uint(10), "test", "one"})

	// Insert a new tuple { 11, 1 }
	resp, err = client.Insert("test", &Tuple{Id: 11, Msg: "test", Name: "one"})

	// Delete a tuple with primary key { 10 }
	resp, err = client.Delete(spaceNo, indexNo, []interface{}{uint(10)})
	// the same:
	resp, err = client.Delete("test", "primary", tarantool.UintKey{10})

	// Replace tuple with primary key 13.
	// Note, Tuple is defined within tests, and has EncdodeMsgpack and
	// DecodeMsgpack methods.
	resp, err = client.Replace(spaceNo, []interface{}{uint(13), 1})
	// the same:
	resp, err = client.Replace("test", []interface{}{uint(13), 1})

	// Update tuple with primary key { 13 }, incrementing second field by 3
	resp, err = client.Update("test", "primary", tarantool.UintKey{13}, []tarantool.Op{{"+", 1, 3}})
	// the same:
	resp, err = client.Update(spaceNo, indexNo, []interface{}{uint(13)}, []interface{}{[]interface{}{"+", 1, 3}})

	// Select just one tuple with primary key { 15 }
	resp, err = client.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{uint(15)})
	// the same:
	resp, err = client.Select("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{15})

	// Select tuples by condition ( primary key > 15 ) with offset 7 and limit 5.
	// BTREE index is supposed.
	resp, err = client.Select(spaceNo, indexNo, 7, 5, tarantool.IterGt, []interface{}{uint(15)})
	// the same:
	resp, err = client.Select("test", "primary", 7, 5, tarantool.IterGt, []interface{}{uint(15)})

	// Call function 'func_name' with arguments
	resp, err = client.Call17("simple_incr", []interface{}{1})

	// Run raw Lua code
	resp, err = client.Eval("return 1 + 2", []interface{}{})

	// Replace
	resp, err = client.Replace("test", &Tuple{Id: 11, Msg: "test", Name: "eleven"})
	// the same:
	resp, err = client.Replace("test", &Tuple{Id: 12, Msg: "test", Name: "twelve"})

	var futs [3]*tarantool.Future
	futs[0] = client.SelectAsync("test", "primary", 0, 2, tarantool.IterLe, tarantool.UintKey{12})
	futs[1] = client.SelectAsync("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{13})
	futs[2] = client.SelectAsync("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{15})
	var t []Tuple
	err = futs[0].GetTyped(&t)
	fmt.Println("Fut", 0, "Error", err)
	fmt.Println("Fut", 0, "Data", t)

	resp, err = futs[1].Get()
	fmt.Println("Fut", 1, "Error", err)
	fmt.Println("Fut", 1, "Data", resp.Data)

	resp, err = futs[2].Get()
	fmt.Println("Fut", 2, "Error", err)
	fmt.Println("Fut", 2, "Data", resp.Data)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
	// Fut 0 Error <nil>
	// Fut 0 Data [{{} 12 test twelve} {{} 11 test eleven}]
	// Fut 1 Error <nil>
	// Fut 1 Data [[13 7]]
	// Fut 2 Error <nil>
	// Fut 2 Data [[15 val 15 bla]]
}

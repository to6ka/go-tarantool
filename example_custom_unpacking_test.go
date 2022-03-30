package tarantool_test

import (
	"fmt"
	"github.com/tarantool/go-tarantool"
	"gopkg.in/vmihailenco/msgpack.v2"
	"log"
	"time"
)

type Twin struct {
	Name  string
	Nonce string
	Val   uint
}

type TweedledeeTuple struct {
	Cid     uint
	Orig    string
	Members []Twin
}

/* Same effect in a "magic" way, but slower */
type TweedledumTuple struct {
	_msgpack struct{} `msgpack:",asArray"`

	Cid     uint
	Orig    string
	Members []Twin
}

func (m *Twin) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(2); err != nil {
		return err
	}
	if err := e.EncodeString(m.Name); err != nil {
		return err
	}
	if err := e.EncodeUint(m.Val); err != nil {
		return err
	}
	return nil
}

func (m *Twin) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 2 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if m.Name, err = d.DecodeString(); err != nil {
		return err
	}
	if m.Val, err = d.DecodeUint(); err != nil {
		return err
	}
	return nil
}

func (c *TweedledeeTuple) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint(c.Cid); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	if err := e.EncodeSliceLen(len(c.Members)); err != nil {
		return err
	}
	for _, m := range c.Members {
		e.Encode(m)
	}
	return nil
}

func (c *TweedledeeTuple) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if c.Cid, err = d.DecodeUint(); err != nil {
		return err
	}
	if c.Orig, err = d.DecodeString(); err != nil {
		return err
	}
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	c.Members = make([]Twin, l)
	for i := 0; i < l; i++ {
		d.Decode(&c.Members[i])
	}
	return nil
}

// Example demonstrates how to use custom (un)packing with typed selects and
// function calls.
//
// You can specify custom pack/unpack functions for your types. This will allow
// you to store complex structures inside a tuple and may speed up you requests.
//
// Alternatively, you can just instruct the `msgpack` library to encode your
// structure as an array. This is safe "magic". It will be easier to implement than
// a custom packer/unpacker, but it will work slower.
func Example_customUnpacking() {
	// Establish connection ...
	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	spaceNo := uint32(524)
	indexNo := uint32(0)

	tuple := TweedledeeTuple{777, "orig", []Twin{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Replace(spaceNo, tuple) // NOTE: insert structure itself
	if err != nil {
		log.Fatalf("Failed to insert: %s", err.Error())
		return
	}

	var tuples []TweedledeeTuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{777}, &tuples)
	if err != nil {
		log.Fatalf("Failed to SelectTyped: %s", err.Error())
		return
	}

	// Same result in a "magic" way
	var tuples2 []TweedledumTuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{777}, &tuples2)
	if err != nil {
		log.Fatalf("Failed to SelectTyped: %s", err.Error())
		return
	}

	// Call function 'func_name' returning a table of custom tuples
	var tuples3 []TweedledeeTuple
	err = conn.CallTyped("func_name", []interface{}{1, 2, 3}, &tuples3)
	if err != nil {
		log.Fatalf("Failed to CallTyped: %s", err.Error())
		return
	}
}

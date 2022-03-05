package datetime_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	. "github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// There is no way to skip tests in testing.M,
// so we use this variable to pass info
// to each testing.T that it should skip.
var isDatetimeSupported = false

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var space = "testDatetime"
var index = "primary"

type TupleDatetime struct {
	tm time.Time
}

func (t *TupleDatetime) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}

	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	t.tm = res.(time.Time)

	return nil
}

func connectWithValidation(t *testing.T) *Connection {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
	}
	return conn
}

func tupleValueIsDatetime(t *testing.T, tuples []interface{}, tm time.Time) {
	if tpl, ok := tuples[0].([]interface{}); !ok {
		t.Errorf("Unexpected return value body")
	} else {
		if len(tpl) != 1 {
			t.Errorf("Unexpected return value body (tuple len = %d)", len(tpl))
		}
		if val, ok := tpl[0].(time.Time); !ok || val != tm {
			fmt.Println("Tuple:   ", tpl[0])
			fmt.Println("Expected:", val)
			t.Errorf("Unexpected return value body (tuple 0 field)")
		}
	}
}

func tupleInsertSelectDelete(t *testing.T, conn *Connection, datetime string) {
	tm, err := time.Parse(time.RFC3339, datetime)
	if err != nil {
		t.Errorf("Time (%s) parse failed: %s", datetime, err)
	}

	// Insert tuple with datetime.
	resp, err := conn.Insert(space, []interface{}{tm})
	if err != nil {
		t.Errorf("Datetime insert failed: %s", err.Error())
	}
	fmt.Println(resp)

	// Select tuple with datetime.
	var offset uint32 = 0
	var limit uint32 = 1
	resp, err = conn.Select(space, index, offset, limit, IterEq, []interface{}{tm})
	if err != nil {
		t.Errorf("Datetime select failed: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	tupleValueIsDatetime(t, resp.Data, tm)

	// Delete tuple with datetime.
	resp, err = conn.Delete(space, index, []interface{}{tm})
	if err != nil {
		t.Errorf("Datetime delete failed: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}
	tupleValueIsDatetime(t, resp.Data, tm)
}

var datetimes = []string{
	"1970-01-01T00:00:00.000000010Z",
	"2010-08-12T11:39:14Z",
	"1984-03-24T18:04:05Z",
	"1970-01-01T00:00:00Z",
	"2010-01-12T00:00:00Z",
	"1970-01-01T00:00:00Z",
	"1970-01-01T00:00:00.123456789Z",
	"1970-01-01T00:00:00.12345678Z",
	"1970-01-01T00:00:00.1234567Z",
	"1970-01-01T00:00:00.123456Z",
	"1970-01-01T00:00:00.12345Z",
	"1970-01-01T00:00:00.1234Z",
	"1970-01-01T00:00:00.123Z",
	"1970-01-01T00:00:00.12Z",
	"1970-01-01T00:00:00.1Z",
	"1970-01-01T00:00:00.01Z",
	"1970-01-01T00:00:00.001Z",
	"1970-01-01T00:00:00.0001Z",
	"1970-01-01T00:00:00.00001Z",
	"1970-01-01T00:00:00.000001Z",
	"1970-01-01T00:00:00.0000001Z",
	"1970-01-01T00:00:00.00000001Z",
	"1970-01-01T00:00:00.000000001Z",
	"1970-01-01T00:00:00.000000009Z",
	"1970-01-01T00:00:00.00000009Z",
	"1970-01-01T00:00:00.0000009Z",
	"1970-01-01T00:00:00.000009Z",
	"1970-01-01T00:00:00.00009Z",
	"1970-01-01T00:00:00.0009Z",
	"1970-01-01T00:00:00.009Z",
	"1970-01-01T00:00:00.09Z",
	"1970-01-01T00:00:00.9Z",
	"1970-01-01T00:00:00.99Z",
	"1970-01-01T00:00:00.999Z",
	"1970-01-01T00:00:00.9999Z",
	"1970-01-01T00:00:00.99999Z",
	"1970-01-01T00:00:00.999999Z",
	"1970-01-01T00:00:00.9999999Z",
	"1970-01-01T00:00:00.99999999Z",
	"1970-01-01T00:00:00.999999999Z",
	"1970-01-01T00:00:00.0Z",
	"1970-01-01T00:00:00.00Z",
	"1970-01-01T00:00:00.000Z",
	"1970-01-01T00:00:00.0000Z",
	"1970-01-01T00:00:00.00000Z",
	"1970-01-01T00:00:00.000000Z",
	"1970-01-01T00:00:00.0000000Z",
	"1970-01-01T00:00:00.00000000Z",
	"1970-01-01T00:00:00.000000000Z",
	"1973-11-29T21:33:09Z",
	"2013-10-28T17:51:56Z",
	"9999-12-31T23:59:59Z",
}

func TestDatetimeInsertSelectDelete(t *testing.T) {
	if isDatetimeSupported == false {
		t.Skip("Skipping test for Tarantool without datetime support in msgpack")
	}

	conn := connectWithValidation(t)
	defer conn.Close()

	for _, dt := range datetimes {
		tupleInsertSelectDelete(t, conn, dt)
	}
}

func TestDatetimeReplace(t *testing.T) {
	if isDatetimeSupported == false {
		t.Skip("Skipping test for Tarantool without datetime support in msgpack")
	}

	conn := connectWithValidation(t)
	defer conn.Close()

	tm, err := time.Parse(time.RFC3339, "2007-01-02T15:04:05Z")
	if err != nil {
		t.Errorf("Time parse failed: %s", err)
	}

	resp, err := conn.Replace(space, []interface{}{tm})
	if err != nil {
		t.Errorf("Datetime replace failed: %s", err)
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}
	tupleValueIsDatetime(t, resp.Data, tm)

	resp, err = conn.Select(space, index, 0, 1, IterEq, []interface{}{tm})
	if err != nil {
		t.Errorf("Datetime select failed: %s", err)
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	tupleValueIsDatetime(t, resp.Data, tm)

	// Delete tuple with datetime.
	resp, err = conn.Delete(space, index, []interface{}{tm})
	if err != nil {
		t.Errorf("Datetime delete failed: %s", err.Error())
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Failed to extract Tarantool version: %s", err)
	}

	if isLess {
		log.Println("Skipping datetime tests...")
		isDatetimeSupported = false
		return m.Run()
	} else {
		isDatetimeSupported = true
	}

	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   "config.lua",
		Listen:       server,
		WorkDir:      "work_dir",
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Fatalf("Failed to prepare test Tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}

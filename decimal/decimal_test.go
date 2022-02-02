package decimal_test

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	. "github.com/tarantool/go-tarantool"
	. "github.com/tarantool/go-tarantool/decimal"
	"github.com/tarantool/go-tarantool/test_helpers"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// There is no way to skip tests in testing.M,
// so we use this variable to pass info
// to each testing.T that it should skip.
var isDecimalSupported = false

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var space = "testDecimal"
var index = "primary"

type TupleDecimal struct {
	number decimal.Decimal
}

/*
var decNumbers = []struct {
	num string
	hex []byte
}{
	{"-12.34", "d6010201234d"},
	{"123.456789000000000", "c70b010f0123456789000000000c"},
	{"0", "d501000c"},
	{"-0", "d501000d"},
	{"1", "d501001c"},
	{"-1", "d501001d"},
	{"0.1", "d501011c"},
	{"-0.1", "d501011d"},
	{"2.718281828459045", "c70a010f02718281828459045c"},
	{"-2.718281828459045", "c70a010f02718281828459045d"},
	{"3.141592653589793", "c70a010f03141592653589793c"},
	{"-3.141592653589793", "c70a010f03141592653589793d"},
	{"1234567891234567890.0987654321987654321", "c7150113012345678912345678900987654321987654321c"},
	{"-1234567891234567890.0987654321987654321", "c7150113012345678912345678900987654321987654321d"},
	{"0.0000000000000000000000000000000000001", "d501251c"},
	{"-0.0000000000000000000000000000000000001", "d501251d"},
	{"0.00000000000000000000000000000000000001", "d501261c"},
	{"-0.00000000000000000000000000000000000001", "d501261d"},
	{"99999999999999999999999999999999999999", "c7150100099999999999999999999999999999999999999c"},
	{"-99999999999999999999999999999999999999", "c7150100099999999999999999999999999999999999999d"},
}
*/

func (t *TupleDecimal) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("Array length doesn't match: %d", l)
	}

	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	t.number = res.(decimal.Decimal)

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

func tupleValueIsDecimal(t *testing.T, tuples []interface{}, number decimal.Decimal) {
	if len(tuples) != 1 {
		t.Errorf("Response Data len (%d) != 1", len(tuples))
	}

	if tpl, ok := tuples[0].([]interface{}); !ok {
		t.Errorf("Unexpected return value body")
	} else {
		if len(tpl) != 1 {
			t.Errorf("Unexpected return value body (tuple len)")
		}
		if val, ok := tpl[0].(decimal.Decimal); !ok || !val.Equal(number) {
			t.Errorf("Unexpected return value body (tuple 0 field)")
		}
	}
}

var decimalBCD = []struct {
	num string
	bcd []byte
}{
	// | length | MP_DECIMAL | scale |  1   | 0 (plus) |
	// |  0x03  |    0x01    | 0x24  | 0x01 | 0x0c     |
	//{"0.000000000000000000000000000000000010", []byte{0x03, 0x01, 0x24, 0x01, 0x0c}},
	// | MP_DECIMAL | scale |  1   |  2,3 |  4 (minus) |
	// |    0x01    | 0x02  | 0x01 | 0x23 | 0x4d       |
	{"-12.34", []byte{0x02, 0x01, 0x23, 0x4b}},
}

func TestMPEncodeNumberToBCD(t *testing.T) {
	for _, tt := range decimalBCD {
		t.Run(tt.num, func(t *testing.T) {
			buf := MPEncodeNumberToBCD(tt.num)
			if reflect.DeepEqual(buf, tt.bcd) != true {
				t.Errorf("Failed to encode decimal (%s) to BCD (%x), expected '%x'", tt.num, buf, tt.bcd)
			}
		})
	}
}

func TestMPDecodeNumberFromBCD(t *testing.T) {
	for _, tt := range decimalBCD {
		t.Run(tt.num, func(t *testing.T) {
			dec_expected, err := decimal.NewFromString(tt.num)
			if err != nil {
				t.Errorf("Failed to encode string to decimal")
			}
			num, err := MPDecodeNumberFromBCD(tt.bcd)
			if err != nil {
				t.Errorf("Failed to decode decimal (%s) from BCD (%x) - '%x'", tt.num, tt.bcd, num)
			}
			dec_actual, err := decimal.NewFromString(strings.Join(num, ""))
			if err != nil {
				t.Errorf("Failed to encode string to decimal")
			}
			if !dec_expected.Equal(dec_actual) {
				t.Errorf("Failed to decode decimal (%s) from BCD (%x) - '%x'", tt.num, tt.bcd, num)
			}
		})
	}
}

func BenchmarkMPEncodeNumberToBCD(b *testing.B) {
	for n := 0; n < b.N; n++ {
		MPEncodeNumberToBCD("-12.34")
	}
}

func BenchmarkMPDecodeNumberFromBCD(b *testing.B) {
	buf := []byte{0x02, 0x01, 0x23, 0x4b}
	for n := 0; n < b.N; n++ {
		MPDecodeNumberFromBCD(buf)
	}
}

func TestSelect(t *testing.T) {
	t.Skip("Broken")

	if isDecimalSupported == false {
		t.Skip("Skipping test for Tarantool without decimal support in msgpack")
	}

	conn := connectWithValidation(t)
	defer conn.Close()

	number, err := decimal.NewFromString("-12.34")
	if err != nil {
		t.Errorf("Failed to prepare test decimal: %s", err)
	}

	var offset uint32 = 0
	var limit uint32 = 1
	resp, err := conn.Select(space, index, offset, limit, IterEq, []interface{}{number})
	if err != nil {
		t.Errorf("Decimal select failed: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	fmt.Println(resp.Data)
	//tupleValueIsDecimal(t, resp.Data, number)
}

func TestInsert(t *testing.T) {
	//t.Skip("Not implemented")
	if isDecimalSupported == false {
		t.Skip("Skipping test for Tarantool without decimal support in msgpack")
	}

	conn := connectWithValidation(t)
	defer conn.Close()

	number, err := decimal.NewFromString("-12.34")
	if err != nil {
		t.Errorf("Failed to prepare test decimal: %s", err)
	}

	resp, err := conn.Insert(space, []interface{}{number})
	if err != nil {
		t.Errorf("Decimal replace failed: %s", err)
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}
	tupleValueIsDecimal(t, resp.Data, number)

	resp, err = conn.Delete(space, index, []interface{}{number})
	if err != nil {
		t.Errorf("Decimal delete failed: %s", err)
	}
	tupleValueIsDecimal(t, resp.Data, number)
}

func TestReplace(t *testing.T) {
	//t.Skip("Not imeplemented")

	if isDecimalSupported == false {
		t.Skip("Skipping test for Tarantool without decimal support in msgpack")
	}

	conn := connectWithValidation(t)
	defer conn.Close()

	number, err := decimal.NewFromString("-12.34")
	if err != nil {
		t.Errorf("Failed to prepare test decimal: %s", err)
	}

	respRep, errRep := conn.Replace(space, []interface{}{number})
	if errRep != nil {
		t.Errorf("Decimal replace failed: %s", errRep)
	}
	if respRep == nil {
		t.Errorf("Response is nil after Replace")
	}
	//tupleValueIsDecimal(t, respRep.Data, number)

	respSel, errSel := conn.Select(space, index, 0, 1, IterEq, []interface{}{number})
	if errSel != nil {
		t.Errorf("Decimal select failed: %s", errSel)
	}
	if respSel == nil {
		t.Errorf("Response is nil after Select")
	}
	//tupleValueIsDecimal(t, respSel.Data, number)
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 2, 0)
	if err != nil {
		log.Fatalf("Failed to extract Tarantool version: %s", err)
	}

	if isLess {
		log.Println("Skipping decimal tests...")
		isDecimalSupported = false
		return m.Run()
	} else {
		isDecimalSupported = true
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

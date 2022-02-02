package decimal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/shopspring/decimal"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// Decimal external type
// Supported since Tarantool 2.2. See more details in issue
// https://github.com/tarantool/tarantool/issues/692
//
// Documentation:
// https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-decimal-type

const Decimal_extId = 1

func encodeDecimal(e *msgpack.Encoder, v reflect.Value) error {
	number := v.Interface().(decimal.Decimal)
	dec := number.String()
	bcdBuf := MPEncodeNumberToBCD(dec)
	_, err := e.Writer().Write(bcdBuf)
	if err != nil {
		return fmt.Errorf("msgpack: can't write bytes to encoder writer: %w", err)
	}

	return nil
}

func decodeDecimal(d *msgpack.Decoder, v reflect.Value) error {
	var bytesCount int = 4 // FIXME
	b := make([]byte, bytesCount)

	_, err := d.Buffered().Read(b)
	if err != nil {
		return fmt.Errorf("msgpack: can't read bytes on decimal decode: %w", err)
	}

	digits, err := MPDecodeNumberFromBCD(b)
	if err != nil {
		return err
	}
	str := strings.Join(digits, "")
	dec, err := decimal.NewFromString(str)
	if err != nil {
		return err
	}

	v.Set(reflect.ValueOf(dec))

	return nil
}

func init() {
	msgpack.Register(reflect.TypeOf((*decimal.Decimal)(nil)).Elem(), encodeDecimal, decodeDecimal)
	msgpack.RegisterExt(Decimal_extId, (*decimal.Decimal)(nil))
}

/**
 * Datetime MessagePack serialization schema is an MP_EXT extension, which
 * creates container of 8 or 16 bytes long payload.
 *
 * +---------+--------+===============+-------------------------------+
 * |0xd7/0xd8|type (4)| seconds (8b)  | nsec; tzoffset; tzindex; (8b) |
 * +---------+--------+===============+-------------------------------+
 *
 * MessagePack data encoded using fixext8 (0xd7) or fixext16 (0xd8), and may
 * contain:
 *
 * - [required] seconds parts as full, unencoded, signed 64-bit integer, stored
 *   in little-endian order;
 *
 * - [optional] all the other fields (nsec, tzoffset, tzindex) if any of them
 *   were having not 0 value. They are packed naturally in little-endian order;
 *
 */

package datetime

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"encoding/binary"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// Datetime external type
// Supported since Tarantool 2.10. See more details in issue
// https://github.com/tarantool/tarantool/issues/5946
const Datetime_extId = 4

/**
 * datetime structure keeps number of seconds and
 * nanoseconds since Unix Epoch.
 * Time is normalized by UTC, so time-zone offset
 * is informative only.
 */
type datetime struct {
	// Seconds since Epoch
	seconds int64
	// Nanoseconds, fractional part of seconds
	nsec int32
	// Timezone offset in minutes from UTC
	// (not implemented in Tarantool, https://github.com/tarantool/tarantool/issues/6751)
	tzOffset int16
	// Olson timezone id
	// (not implemented in Tarantool, https://github.com/tarantool/tarantool/issues/6751)
	tzIndex int16
}

const (
	secondsSize  = 8
	nsecSize     = 4
	tzIndexSize  = 2
	tzOffsetSize = 2
)

func encodeDatetime(e *msgpack.Encoder, v reflect.Value) error {
	var dt datetime

	tm := v.Interface().(time.Time)
	dt.seconds = tm.Unix()
	nsec := tm.Nanosecond()
	dt.nsec = int32(math.Round((10000 * float64(nsec)) / 10000))
	dt.tzIndex = 0  /* not implemented */
	dt.tzOffset = 0 /* not implemented */

	var bytesSize = secondsSize
	if dt.nsec != 0 || dt.tzOffset != 0 || dt.tzIndex != 0 {
		bytesSize += nsecSize + tzIndexSize + tzOffsetSize
	}

	buf := make([]byte, bytesSize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(dt.seconds))
	if bytesSize == 16 {
		binary.LittleEndian.PutUint32(buf[secondsSize:], uint32(dt.nsec))
		binary.LittleEndian.PutUint16(buf[nsecSize:], uint16(dt.tzOffset))
		binary.LittleEndian.PutUint16(buf[tzOffsetSize:], uint16(dt.tzIndex))
	}

	_, err := e.Writer().Write(buf)
	if err != nil {
		return fmt.Errorf("msgpack: can't write bytes to encoder writer: %w", err)
	}

	return nil
}

func decodeDatetime(d *msgpack.Decoder, v reflect.Value) error {
	var dt datetime
	secondsBytes := make([]byte, secondsSize)
	n, err := d.Buffered().Read(secondsBytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't read bytes on datetime's seconds decode: %w", err)
	}
	if n < secondsSize {
		return fmt.Errorf("msgpack: unexpected end of stream after %d datetime bytes", n)
	}
	dt.seconds = int64(binary.LittleEndian.Uint64(secondsBytes))
	dt.nsec = 0
	tailSize := nsecSize + tzOffsetSize + tzIndexSize
	tailBytes := make([]byte, tailSize)
	n, err = d.Buffered().Read(tailBytes)
	// Part with nanoseconds, tzoffset and tzindex is optional,
	// so we don't need to handle an error here.
	if err == nil {
		if n < tailSize {
			return fmt.Errorf("msgpack: can't read bytes on datetime's tail decode: %w", err)
		}
		dt.nsec = int32(binary.LittleEndian.Uint32(tailBytes[0:]))
		dt.tzOffset = int16(binary.LittleEndian.Uint16(tailBytes[nsecSize:]))
		dt.tzIndex = int16(binary.LittleEndian.Uint16(tailBytes[tzOffsetSize:]))
	}
	t := time.Unix(dt.seconds, int64(dt.nsec)).UTC()
	v.Set(reflect.ValueOf(t))

	return nil
}

func init() {
	msgpack.Register(reflect.TypeOf((*time.Time)(nil)).Elem(), encodeDatetime, decodeDatetime)
	msgpack.RegisterExt(Datetime_extId, (*time.Time)(nil))
}

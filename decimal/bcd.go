package decimal

import (
	_ "fmt"
	"strconv"
	"errors"
)

var (
	ErrNumberIsNotADecimal = errors.New("Number is not a decimal.")
	ErrWrongExponentaRange = errors.New("Exponenta has a wrong range.")
)

var mpDecimalSign = map[rune]byte{
	'+': 0x0a,
	'-': 0x0b,
}

var mpIsDecimalNegative = map[byte]bool{
	0x0a: false,
	0x0b: true,
	0x0c: false,
	0x0d: true,
	0x0e: false,
	0x0f: false,
}

var hex_digit = map[rune]byte{
	'1': 0x1,
	'2': 0x2,
	'3': 0x3,
	'4': 0x4,
	'5': 0x5,
	'6': 0x6,
	'7': 0x7,
	'8': 0x8,
	'9': 0x9,
	'0': 0x0,
}

func MPEncodeNumberToBCD(buf string) []byte {
	scale := 0
	sign := '+'
	// TODO: The first nibble contains 0 if the decimal number has an even number of digits.
	nibbleIdx := 2 /* First nibble is for sign */
	byteBuf := make([]byte, 1)
	for i, ch := range buf {
		// TODO: ignore leading zeroes
		// Check for sign in a first nibble.
		if (i == 0) && (ch == '-' || ch == '+') {
			sign = ch
			continue
		}

		// Remember a number of digits after the decimal point.
		if ch == '.' {
			scale = len(buf) - i - 1
			continue
		}

		//digit := byte(ch)
		digit := hex_digit[ch]
		//fmt.Printf("DEBUG: ch %c\n", ch)
		//fmt.Printf("DEBUG: digit byte %x\n", digit)
		highNibble := nibbleIdx%2 != 0
		lowByte := len(byteBuf) - 1
		if highNibble {
			digit = digit << 4
			byteBuf = append(byteBuf, digit)
		} else {
			if nibbleIdx == 2 {
				byteBuf[0] = digit
			} else {
				byteBuf[lowByte] = byteBuf[lowByte] | digit
			}
		}
		//fmt.Printf("DEBUG: %x\n", byteBuf)
		nibbleIdx += 1
	}
	if nibbleIdx%2 != 0 {
		byteBuf = append(byteBuf, mpDecimalSign[sign])
	} else {
		lowByte := len(byteBuf) - 1
		byteBuf[lowByte] = byteBuf[lowByte] | mpDecimalSign[sign]
	}
	byteBuf = append([]byte{byte(scale)}, byteBuf...)
	//fmt.Printf("DEBUG: Encoded byteBuf %x\n", byteBuf)

	return byteBuf
}

func highNibble(b byte) byte {
	return b >> 4
}

func lowNibble(b byte) byte {
	return b & 0x0f
}

// TODO: BitReader https://go.dev/play/p/Wyr_K9YAro
// The first byte of the BCD array contains the first digit of the number.
// The first nibble contains 0 if the decimal number has an even number of digits.
// The last byte of the BCD array contains the last digit of the number
// and the final nibble that represents the number's sign.
func MPDecodeNumberFromBCD(bcdBuf []byte) ([]string, error) {
	// Maximum decimal digits taken by a decimal representation.
	const DecimalMaxDigits = 38
	const mpScaleIdx = 0

	scale := int32(bcdBuf[mpScaleIdx])
	// scale == -exponent, the exponent must be in range
	// [ -DecimalMaxDigits; DecimalMaxDigits )
	if scale < -DecimalMaxDigits || scale >= DecimalMaxDigits {
		return nil, ErrWrongExponentaRange
	}

	bcdBuf = bcdBuf[mpScaleIdx+1:]
	//fmt.Printf("DEBUG: MPDecodeNumberFromBCD %x\n", bcdBuf)
	length := len(bcdBuf)
	var digits []string
	for i, bcd_byte := range bcdBuf {
		// Skip leading zeros.
		if len(digits) == 0 && int(bcd_byte) == 0 {
			continue
		}
		if high := highNibble(bcd_byte); high != 0 {
			digit := strconv.Itoa(int(high))
			digits = append(digits, digit)
		}
		low := lowNibble(bcd_byte)
		if int(i) != length-1 {
			digit := strconv.Itoa(int(low))
			digits = append(digits, digit)
		}
		/* TODO: Make sure every digit is less than 9 and bigger than 0 */
	}

	digits = append(digits[:scale+1], digits[scale:]...)
	digits[scale] = "."
	last_byte := bcdBuf[length-1]
	sign := lowNibble(last_byte)
	if mpIsDecimalNegative[sign] {
		digits = append([]string{"-"}, digits...)
	}

	return digits, nil
}

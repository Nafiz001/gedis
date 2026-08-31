// Package resp implements the RESP2 wire protocol used by Redis.
//
// The protocol is deliberately simple: every message begins with a single byte
// that identifies its type, and every message is terminated by CRLF.
//
//	+OK\r\n              simple string
//	-ERR bad thing\r\n   error
//	:42\r\n              integer
//	$5\r\nhello\r\n      bulk string ($-1\r\n is the null bulk string)
//	*2\r\n...            array       (*-1\r\n is the null array)
//
// Clients send commands as an array of bulk strings; servers reply with any of
// the five types.
package resp

import "fmt"

// Type is the leading byte that identifies a RESP value.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

func (t Type) String() string {
	switch t {
	case TypeSimpleString:
		return "simple string"
	case TypeError:
		return "error"
	case TypeInteger:
		return "integer"
	case TypeBulkString:
		return "bulk string"
	case TypeArray:
		return "array"
	default:
		return fmt.Sprintf("unknown type %q", byte(t))
	}
}

// Value is a decoded RESP value. Only the field matching Type is meaningful.
//
// The server's hot path does not allocate Values -- it reads commands straight
// into a [][]byte and writes replies directly to a buffer. Value exists for the
// client side of replication and for tests, where convenience beats speed.
type Value struct {
	Type Type

	// Str holds the payload of a simple string, an error, or a bulk string.
	Str []byte
	// Int holds the payload of an integer.
	Int int64
	// Array holds the elements of an array.
	Array []Value
	// Null marks a null bulk string ($-1) or a null array (*-1).
	Null bool
}

// String renders a Value the way redis-cli would display it. Test-friendly.
func (v Value) String() string {
	if v.Null {
		return "(nil)"
	}
	switch v.Type {
	case TypeSimpleString, TypeBulkString:
		return string(v.Str)
	case TypeError:
		return "(error) " + string(v.Str)
	case TypeInteger:
		return fmt.Sprintf("(integer) %d", v.Int)
	case TypeArray:
		out := "["
		for i, e := range v.Array {
			if i > 0 {
				out += " "
			}
			out += e.String()
		}
		return out + "]"
	default:
		return "(invalid)"
	}
}

// Constructors for the values the server replies with.

func SimpleString(s string) Value { return Value{Type: TypeSimpleString, Str: []byte(s)} }
func Error(s string) Value        { return Value{Type: TypeError, Str: []byte(s)} }
func Integer(n int64) Value       { return Value{Type: TypeInteger, Int: n} }
func BulkString(b []byte) Value   { return Value{Type: TypeBulkString, Str: b} }
func Array(vs ...Value) Value     { return Value{Type: TypeArray, Array: vs} }
func NullBulk() Value             { return Value{Type: TypeBulkString, Null: true} }
func NullArray() Value            { return Value{Type: TypeArray, Null: true} }

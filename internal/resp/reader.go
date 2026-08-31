package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Protocol limits, matching the real Redis server. They exist so a hostile or
// buggy client cannot make the server allocate unbounded memory from a single
// header line.
const (
	MaxBulkLength      = 512 * 1024 * 1024 // 512 MB, Redis' proto-max-bulk-len
	MaxMultiBulkLength = 1024 * 1024       // elements in one command
	MaxInlineLength    = 64 * 1024         // an inline command line
)

// ErrProtocol is a malformed-input error. The server replies with it and then
// closes the connection, because once framing is lost the stream is unusable.
type ErrProtocol struct{ Msg string }

func (e *ErrProtocol) Error() string { return "Protocol error: " + e.Msg }

func protoErr(format string, args ...any) error {
	return &ErrProtocol{Msg: fmt.Sprintf(format, args...)}
}

// Reader decodes RESP values and commands from a stream.
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 16*1024)}
}

// NewReaderSize is used by benchmarks and by the replication client, which
// benefit from a larger buffer.
func NewReaderSize(r io.Reader, size int) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, size)}
}

// readLine returns one CRLF-terminated line with the terminator stripped.
//
// bufio.ReadSlice returns a view into the internal buffer, which is only valid
// until the next read -- that is exactly what we want for header lines, since
// we parse them into ints immediately. When a line is longer than the buffer,
// ReadSlice reports ErrBufferFull and we fall back to copying.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		buf := append([]byte(nil), line...)
		for errors.Is(err, bufio.ErrBufferFull) {
			if len(buf) > MaxInlineLength {
				return nil, protoErr("too big inline request")
			}
			line, err = r.br.ReadSlice('\n')
			buf = append(buf, line...)
		}
		line = buf
	}
	if err != nil {
		return nil, err
	}
	// A well-formed line is at least "\r\n".
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErr("unbalanced line terminator")
	}
	return line[:len(line)-2], nil
}

// readInt parses the numeric argument of a header line such as "$5" or "*2".
func (r *Reader) readInt() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, protoErr("invalid multibulk length")
	}
	return n, nil
}

// ReadCommand reads one client command and returns its arguments, with the
// command name in element 0. The returned slices are freshly allocated and safe
// to retain.
//
// Two encodings are accepted. Real clients send the multibulk form. The inline
// form -- a bare space-separated line -- is what you get when someone debugs the
// server over telnet or netcat, and Redis supports it, so we do too.
func (r *Reader) ReadCommand() ([][]byte, error) {
	b, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] != byte(TypeArray) {
		return r.readInlineCommand()
	}
	if _, err := r.br.Discard(1); err != nil {
		return nil, err
	}

	n, err := r.readInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n <= 0:
		// "*0\r\n" and "*-1\r\n" are empty commands; the caller loops again.
		return nil, nil
	case n > MaxMultiBulkLength:
		return nil, protoErr("invalid multibulk length")
	}

	args := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		b, err := r.br.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != byte(TypeBulkString) {
			return nil, protoErr("expected '$', got '%c'", b)
		}
		arg, err := r.readBulkBody()
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, protoErr("unexpected null bulk string in command")
		}
		args = append(args, arg)
	}
	return args, nil
}

// readInlineCommand parses a telnet-style command line.
func (r *Reader) readInlineCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	return splitInline(line)
}

// escapeChar is the backslash that introduces an escape sequence inside a
// double-quoted inline argument.
const escapeChar = 0x5c

// splitInline splits on whitespace, honouring the quoting rules redis-cli uses
// so that `SET greeting "hello world"` works over a raw socket.
func splitInline(line []byte) ([][]byte, error) {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			break
		}
		var arg []byte
		switch line[i] {
		case '"', '\'':
			quote := line[i]
			i++
			for i < len(line) && line[i] != quote {
				if line[i] == escapeChar && quote == '"' && i+1 < len(line) {
					i++
					switch line[i] {
					case 'n':
						arg = append(arg, '\n')
					case 'r':
						arg = append(arg, '\r')
					case 't':
						arg = append(arg, '\t')
					default:
						arg = append(arg, line[i])
					}
				} else {
					arg = append(arg, line[i])
				}
				i++
			}
			if i >= len(line) {
				return nil, protoErr("unbalanced quotes in request")
			}
			i++ // closing quote
		default:
			start := i
			for i < len(line) && line[i] != ' ' && line[i] != '\t' {
				i++
			}
			arg = append([]byte(nil), line[start:i]...)
		}
		args = append(args, arg)
	}
	return args, nil
}

// readBulkBody reads the length line and payload of a bulk string whose '$'
// has already been consumed. It returns nil for the null bulk string.
func (r *Reader) readBulkBody() ([]byte, error) {
	n, err := r.readInt()
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}
	if n < 0 || n > MaxBulkLength {
		return nil, protoErr("invalid bulk length")
	}
	// +2 so the trailing CRLF is consumed in the same read.
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf[:n], nil
}

// ReadValue decodes any RESP value. It is used by the replication client to
// read a master's replies, and by tests to assert on server output.
func (r *Reader) ReadValue() (Value, error) {
	t, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(t) {
	case TypeSimpleString, TypeError:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: Type(t), Str: append([]byte(nil), line...)}, nil

	case TypeInteger:
		n, err := r.readInt()
		if err != nil {
			return Value{}, err
		}
		return Integer(n), nil

	case TypeBulkString:
		body, err := r.readBulkBody()
		if err != nil {
			return Value{}, err
		}
		if body == nil {
			return NullBulk(), nil
		}
		return BulkString(body), nil

	case TypeArray:
		n, err := r.readInt()
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return NullArray(), nil
		}
		if n < 0 || n > MaxMultiBulkLength {
			return Value{}, protoErr("invalid multibulk length")
		}
		out := make([]Value, 0, n)
		for i := int64(0); i < n; i++ {
			v, err := r.ReadValue()
			if err != nil {
				return Value{}, err
			}
			out = append(out, v)
		}
		return Array(out...), nil

	default:
		return Value{}, protoErr("unexpected type byte '%c'", t)
	}
}

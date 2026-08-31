package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// bs is a literal backslash, kept as a constant so test wire data can
// contain escape sequences without fighting Go string escaping.
const bs = string(rune(0x5c))

func readCmd(t *testing.T, wire string) [][]byte {
	t.Helper()
	args, err := NewReader(strings.NewReader(wire)).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand(%q): %v", wire, err)
	}
	return args
}

func joinArgs(args [][]byte) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = string(a)
	}
	return strings.Join(parts, "|")
}

func TestReadCommandMultibulk(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", "PING"},
		{"echo", "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", "ECHO|hello"},
		{"set with options", "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n", "SET|k|v|EX|10"},
		{"empty argument", "*2\r\n$3\r\nGET\r\n$0\r\n\r\n", "GET|"},
		{"binary safe", "*2\r\n$3\r\nSET\r\n$3\r\na\x00b\r\n", "SET|a\x00b"},
		{"embedded crlf", "*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n", "ECHO|a\r\nb"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinArgs(readCmd(t, tc.wire)); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadCommandInline(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want string
	}{
		{"bare", "PING\r\n", "PING"},
		{"spaced", "SET key value\r\n", "SET|key|value"},
		{"extra whitespace", "  SET   key    value  \r\n", "SET|key|value"},
		{"double quoted", "SET greeting \"hello world\"\r\n", "SET|greeting|hello world"},
		{"single quoted", "SET greeting 'hello world'\r\n", "SET|greeting|hello world"},
		{"escape sequences", `SET k "a` + bs + `tb` + bs + `nc"` + "\r\n", "SET|k|a\tb\nc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinArgs(readCmd(t, tc.wire)); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// Pipelined commands must come back one at a time from a single stream.
func TestReadCommandPipelined(t *testing.T) {
	wire := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(strings.NewReader(wire))
	want := []string{"PING", "GET|a", "PING"}
	for i, w := range want {
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if got := joinArgs(args); got != w {
			t.Errorf("command %d: got %q, want %q", i, got, w)
		}
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Errorf("after last command: got %v, want io.EOF", err)
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{"bad multibulk length", "*abc\r\n"},
		{"multibulk too long", "*99999999\r\n"},
		{"element not bulk", "*1\r\n+PING\r\n"},
		{"bad bulk length", "*1\r\n$abc\r\n"},
		{"bulk length too big", "*1\r\n$999999999999\r\n"},
		{"missing crlf after bulk", "*1\r\n$4\r\nPINGXX"},
		{"unbalanced quotes", "SET k \"unterminated\r\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tc.wire)).ReadCommand()
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			// io.EOF is acceptable for truncated input; what must never happen
			// is silently accepting malformed framing.
			var pe *ErrProtocol
			if !errors.As(err, &pe) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("unexpected error kind: %v", err)
			}
		})
	}
}

// An empty multibulk is a no-op, not an error: real clients send it.
func TestReadCommandEmptyMultibulk(t *testing.T) {
	args, err := NewReader(strings.NewReader("*0\r\n")).ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("got %d args, want 0", len(args))
	}
}

// A bulk string larger than the read buffer must be reassembled correctly.
func TestReadCommandLargeBulk(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 128*1024)
	var wire bytes.Buffer
	w := NewWriter(&wire)
	if err := w.WriteArrayLen(2); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulkString("SET"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	args, err := NewReader(&wire).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 2 || !bytes.Equal(args[1], payload) {
		t.Errorf("large bulk did not round-trip")
	}
}

func TestWriterEncoding(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*Writer) error
		want string
	}{
		{"simple string", func(w *Writer) error { return w.WriteSimpleString("OK") }, "+OK\r\n"},
		{"error", func(w *Writer) error { return w.WriteError("ERR nope") }, "-ERR nope\r\n"},
		{"integer", func(w *Writer) error { return w.WriteInt(42) }, ":42\r\n"},
		{"negative integer", func(w *Writer) error { return w.WriteInt(-1) }, ":-1\r\n"},
		{"bulk", func(w *Writer) error { return w.WriteBulkString("hello") }, "$5\r\nhello\r\n"},
		{"empty bulk", func(w *Writer) error { return w.WriteBulkString("") }, "$0\r\n\r\n"},
		{"nil bulk", func(w *Writer) error { return w.WriteBulk(nil) }, "$-1\r\n"},
		{"null bulk", func(w *Writer) error { return w.WriteNullBulk() }, "$-1\r\n"},
		{"null array", func(w *Writer) error { return w.WriteNullArray() }, "*-1\r\n"},
		{"array header", func(w *Writer) error { return w.WriteArrayLen(3) }, "*3\r\n"},
		{"command", func(w *Writer) error { return w.WriteCommand("GET", "k") }, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := tc.fn(w); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// Every value the server can send must survive a write/read round trip.
func TestValueRoundTrip(t *testing.T) {
	values := []Value{
		SimpleString("OK"),
		Error("ERR something went wrong"),
		Integer(0),
		Integer(-9223372036854775808),
		BulkString([]byte("binary\x00safe\r\ndata")),
		BulkString([]byte{}),
		NullBulk(),
		NullArray(),
		Array(Integer(1), BulkString([]byte("two")), NullBulk()),
		Array(),
		Array(Array(Integer(1)), SimpleString("nested")),
	}
	for _, want := range values {
		t.Run(want.String(), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.WriteValue(want); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			got, err := NewReader(&buf).ReadValue()
			if err != nil {
				t.Fatalf("ReadValue: %v", err)
			}
			if got.String() != want.String() {
				t.Errorf("got %s, want %s", got.String(), want.String())
			}
		})
	}
}

func BenchmarkReadCommand(b *testing.B) {
	one := "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
	wire := []byte(strings.Repeat(one, 1000))
	b.SetBytes(int64(len(one)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += 1000 {
		r := NewReader(bytes.NewReader(wire))
		for j := 0; j < 1000; j++ {
			if _, err := r.ReadCommand(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkWriteBulk(b *testing.B) {
	payload := []byte("myvalue")
	w := NewWriter(io.Discard)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteBulk(payload); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
}

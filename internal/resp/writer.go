package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer encodes RESP replies onto a buffered stream.
//
// The reply path is the hottest code in the server, so Writer deliberately has
// no Value-building step: handlers call the typed methods and bytes go straight
// into the buffer. The scratch array keeps integer formatting allocation-free.
type Writer struct {
	bw      *bufio.Writer
	scratch [32]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 16*1024)}
}

func NewWriterSize(w io.Writer, size int) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, size)}
}

// Flush pushes buffered bytes to the underlying stream. The connection loop
// calls this once per batch of pipelined commands rather than once per reply,
// which is where most of the throughput comes from.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered reports how many bytes are waiting to be flushed.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

// writeNumberLine writes a header such as "$5\r\n" or "*2\r\n" without
// allocating.
func (w *Writer) writeNumberLine(prefix byte, n int64) error {
	buf := w.scratch[:0]
	buf = append(buf, prefix)
	buf = strconv.AppendInt(buf, n, 10)
	buf = append(buf, '\r', '\n')
	_, err := w.bw.Write(buf)
	return err
}

func (w *Writer) WriteSimpleString(s string) error {
	if err := w.bw.WriteByte(byte(TypeSimpleString)); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteError writes an error reply. The convention is that the first word is a
// machine-readable code, e.g. "ERR unknown command" or "WRONGTYPE ...".
func (w *Writer) WriteError(s string) error {
	if err := w.bw.WriteByte(byte(TypeError)); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

func (w *Writer) WriteInt(n int64) error {
	return w.writeNumberLine(byte(TypeInteger), n)
}

func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		return w.WriteNullBulk()
	}
	if err := w.writeNumberLine(byte(TypeBulkString), int64(len(b))); err != nil {
		return err
	}
	if _, err := w.bw.Write(b); err != nil {
		return err
	}
	return w.writeCRLF()
}

func (w *Writer) WriteBulkString(s string) error {
	if err := w.writeNumberLine(byte(TypeBulkString), int64(len(s))); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteNullBulk writes the RESP2 null, which is what GET returns for a missing
// key and what redis-cli prints as (nil).
func (w *Writer) WriteNullBulk() error {
	_, err := w.bw.WriteString("$-1\r\n")
	return err
}

func (w *Writer) WriteNullArray() error {
	_, err := w.bw.WriteString("*-1\r\n")
	return err
}

// WriteArrayLen writes an array header; the caller then writes n elements.
// Streaming the header first means a large reply never has to be materialised
// in memory before it is sent.
func (w *Writer) WriteArrayLen(n int) error {
	return w.writeNumberLine(byte(TypeArray), int64(n))
}

// WriteRaw writes pre-encoded RESP bytes. Replication uses it to forward a
// command verbatim to replicas.
func (w *Writer) WriteRaw(b []byte) error {
	_, err := w.bw.Write(b)
	return err
}

// WriteValue writes a decoded Value. Convenient but slower than the typed
// methods; used off the hot path.
func (w *Writer) WriteValue(v Value) error {
	switch v.Type {
	case TypeSimpleString:
		return w.WriteSimpleString(string(v.Str))
	case TypeError:
		return w.WriteError(string(v.Str))
	case TypeInteger:
		return w.WriteInt(v.Int)
	case TypeBulkString:
		if v.Null {
			return w.WriteNullBulk()
		}
		return w.WriteBulk(v.Str)
	case TypeArray:
		if v.Null {
			return w.WriteNullArray()
		}
		if err := w.WriteArrayLen(len(v.Array)); err != nil {
			return err
		}
		for _, e := range v.Array {
			if err := w.WriteValue(e); err != nil {
				return err
			}
		}
		return nil
	default:
		return w.WriteError("ERR internal: cannot encode value")
	}
}

// WriteCommand encodes args as a multibulk command. The replication client uses
// it to talk to a master.
func (w *Writer) WriteCommand(args ...string) error {
	if err := w.WriteArrayLen(len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if err := w.WriteBulkString(a); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeCRLF() error {
	_, err := w.bw.WriteString("\r\n")
	return err
}

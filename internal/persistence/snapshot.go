package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/nafizahmed/gedis/internal/store"
)

// The snapshot file is a compact binary image of the whole keyspace.
//
// Layout:
//
//	"GEDIS"  <version:1>
//	repeated records:
//	  0xFE <varint db>                    switch database
//	  0xFD <int64 le expireAtMillis>      deadline for the next key
//	  <type:1> <varint len><key> <value>  one key
//	0xFF <uint64 le crc64 of everything above>
//
// Lengths are varints because most keys and values are short: a varint spends
// one byte on anything under 128, where a fixed 32-bit length would spend four.
// The trailing CRC-64 covers the entire file, so a truncated or corrupted
// snapshot is detected at load rather than silently restoring partial data --
// the opposite of the AOF, where a truncated tail is expected and tolerated.
const (
	snapshotMagic   = "GEDIS"
	snapshotVersion = 1

	opSelectDB byte = 0xFE
	opExpiry   byte = 0xFD
	opEOF      byte = 0xFF
)

// Value type tags. They are part of the file format, so their numbers are
// fixed: changing one would misread every existing snapshot.
const (
	typeString byte = 0
	typeList   byte = 1
	typeHash   byte = 2
	typeSet    byte = 3
	typeZSet   byte = 4
)

var crcTable = crc64.MakeTable(crc64.ISO)

// SaveSnapshot writes every database to path.
//
// It writes to a temporary file and renames it into place, so a crash partway
// through leaves the previous snapshot intact. A half-written snapshot that
// replaced a good one would be worse than no snapshot at all.
func SaveSnapshot(path string, dbs []*store.Store) (int, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, fmt.Errorf("create snapshot directory: %w", err)
		}
	}

	tempPath := path + ".tmp"
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, fmt.Errorf("create snapshot: %w", err)
	}

	w := &snapshotWriter{bw: bufio.NewWriterSize(f, 256*1024), crc: crc64.New(crcTable)}
	keys, err := w.writeAll(dbs)
	if err == nil {
		err = w.finish()
	}
	if err == nil {
		err = w.flushTo(f)
	}
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}

	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return 0, fmt.Errorf("install snapshot: %w", err)
	}
	return keys, nil
}

type snapshotWriter struct {
	bw  *bufio.Writer
	crc io.Writer
	// scratch avoids an allocation per varint and per fixed-width field.
	scratch [binary.MaxVarintLen64]byte
}

// write sends bytes to both the file buffer and the running checksum.
func (w *snapshotWriter) write(b []byte) error {
	if _, err := w.crc.Write(b); err != nil {
		return err
	}
	_, err := w.bw.Write(b)
	return err
}

func (w *snapshotWriter) writeByte(b byte) error {
	w.scratch[0] = b
	return w.write(w.scratch[:1])
}

func (w *snapshotWriter) writeUvarint(v uint64) error {
	n := binary.PutUvarint(w.scratch[:], v)
	return w.write(w.scratch[:n])
}

func (w *snapshotWriter) writeInt64(v int64) error {
	binary.LittleEndian.PutUint64(w.scratch[:8], uint64(v))
	return w.write(w.scratch[:8])
}

// writeFloat64 stores a score by its IEEE-754 bits, which round-trips exactly
// -- including the infinities sorted sets allow as scores.
func (w *snapshotWriter) writeFloat64(v float64) error {
	binary.LittleEndian.PutUint64(w.scratch[:8], math.Float64bits(v))
	return w.write(w.scratch[:8])
}

func (w *snapshotWriter) writeBlob(b []byte) error {
	if err := w.writeUvarint(uint64(len(b))); err != nil {
		return err
	}
	return w.write(b)
}

func (w *snapshotWriter) writeString(s string) error {
	if err := w.writeUvarint(uint64(len(s))); err != nil {
		return err
	}
	return w.write([]byte(s))
}

func (w *snapshotWriter) writeAll(dbs []*store.Store) (int, error) {
	if err := w.write([]byte(snapshotMagic)); err != nil {
		return 0, err
	}
	if err := w.writeByte(snapshotVersion); err != nil {
		return 0, err
	}

	total := 0
	for dbIndex, db := range dbs {
		if db == nil || db.Len() == 0 {
			continue // empty databases are omitted entirely
		}
		if err := w.writeByte(opSelectDB); err != nil {
			return total, err
		}
		if err := w.writeUvarint(uint64(dbIndex)); err != nil {
			return total, err
		}

		// One shard at a time, so no single lock is held for the whole file.
		for i := 0; i < db.ShardCount(); i++ {
			var shardErr error
			db.ForEachInShard(i, func(key string, value any, expireAt int64) {
				if shardErr != nil {
					return
				}
				if shardErr = w.writeEntry(key, value, expireAt); shardErr == nil {
					total++
				}
			})
			if shardErr != nil {
				return total, shardErr
			}
		}
	}
	return total, nil
}

func (w *snapshotWriter) writeEntry(key string, value any, expireAt int64) error {
	if expireAt != store.NoExpiry {
		if err := w.writeByte(opExpiry); err != nil {
			return err
		}
		if err := w.writeInt64(expireAt); err != nil {
			return err
		}
	}

	switch v := value.(type) {
	case []byte:
		if err := w.writeKeyHeader(typeString, key); err != nil {
			return err
		}
		return w.writeBlob(v)

	case *store.List:
		if err := w.writeKeyHeader(typeList, key); err != nil {
			return err
		}
		items := v.All()
		if err := w.writeUvarint(uint64(len(items))); err != nil {
			return err
		}
		for _, item := range items {
			if err := w.writeBlob(item); err != nil {
				return err
			}
		}
		return nil

	case *store.Hash:
		if err := w.writeKeyHeader(typeHash, key); err != nil {
			return err
		}
		pairs := v.All()
		if err := w.writeUvarint(uint64(len(pairs))); err != nil {
			return err
		}
		for _, p := range pairs {
			if err := w.writeString(p.Field); err != nil {
				return err
			}
			if err := w.writeBlob(p.Value); err != nil {
				return err
			}
		}
		return nil

	case *store.Set:
		if err := w.writeKeyHeader(typeSet, key); err != nil {
			return err
		}
		members := v.Members()
		if err := w.writeUvarint(uint64(len(members))); err != nil {
			return err
		}
		for _, m := range members {
			if err := w.writeString(m); err != nil {
				return err
			}
		}
		return nil

	case *store.ZSet:
		if err := w.writeKeyHeader(typeZSet, key); err != nil {
			return err
		}
		members := v.RangeByRank(0, -1, false)
		if err := w.writeUvarint(uint64(len(members))); err != nil {
			return err
		}
		for _, m := range members {
			if err := w.writeString(m.Member); err != nil {
				return err
			}
			if err := w.writeFloat64(m.Score); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("cannot snapshot key %q: unknown value type %T", key, value)
	}
}

func (w *snapshotWriter) writeKeyHeader(typ byte, key string) error {
	if err := w.writeByte(typ); err != nil {
		return err
	}
	return w.writeString(key)
}

// finish writes the terminator and the checksum. The checksum itself is not
// part of what it covers.
func (w *snapshotWriter) finish() error {
	if err := w.writeByte(opEOF); err != nil {
		return err
	}
	sum := w.crc.(interface{ Sum64() uint64 }).Sum64()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], sum)
	_, err := w.bw.Write(buf[:])
	return err
}

func (w *snapshotWriter) flushTo(f *os.File) error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// --- loading ---

// LoadSnapshot restores a snapshot into dbs. A missing file is not an error:
// it just means there is nothing to restore.
//
// now is the current time in Unix milliseconds; keys whose deadline has already
// passed are dropped rather than loaded, so a server restarted after a long
// downtime does not briefly resurrect expired data.
func LoadSnapshot(path string, dbs []*store.Store, now int64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	r := &snapshotReader{br: bufio.NewReaderSize(f, 256*1024), crc: crc64.New(crcTable)}
	return r.readAll(dbs, now)
}

type snapshotReader struct {
	br  *bufio.Reader
	crc interface {
		io.Writer
		Sum64() uint64
	}
}

// read fills b and folds the bytes into the running checksum.
func (r *snapshotReader) read(b []byte) error {
	if _, err := io.ReadFull(r.br, b); err != nil {
		return err
	}
	_, err := r.crc.Write(b)
	return err
}

func (r *snapshotReader) readByte() (byte, error) {
	var buf [1]byte
	if err := r.read(buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// readUvarint reads a varint byte by byte, feeding each into the checksum.
func (r *snapshotReader) readUvarint() (uint64, error) {
	var value uint64
	var shift uint
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return value | uint64(b)<<shift, nil
		}
		value |= uint64(b&0x7f) << shift
		shift += 7
	}
	return 0, fmt.Errorf("malformed varint")
}

func (r *snapshotReader) readInt64() (int64, error) {
	var buf [8]byte
	if err := r.read(buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}

func (r *snapshotReader) readFloat64() (float64, error) {
	var buf [8]byte
	if err := r.read(buf[:]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[:])), nil
}

// maxSnapshotBlob bounds a single length prefix, so a corrupt file cannot make
// the loader allocate gigabytes before the checksum ever gets checked.
const maxSnapshotBlob = 512 * 1024 * 1024

func (r *snapshotReader) readBlob() ([]byte, error) {
	n, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	if n > maxSnapshotBlob {
		return nil, fmt.Errorf("blob length %d exceeds the maximum", n)
	}
	buf := make([]byte, n)
	if err := r.read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *snapshotReader) readString() (string, error) {
	b, err := r.readBlob()
	return string(b), err
}

// readCount reads a collection length, rejecting absurd values early.
func (r *snapshotReader) readCount() (int, error) {
	n, err := r.readUvarint()
	if err != nil {
		return 0, err
	}
	if n > 1<<32 {
		return 0, fmt.Errorf("collection length %d exceeds the maximum", n)
	}
	return int(n), nil
}

func (r *snapshotReader) readAll(dbs []*store.Store, now int64) (int, error) {
	header := make([]byte, len(snapshotMagic)+1)
	if err := r.read(header); err != nil {
		return 0, fmt.Errorf("read snapshot header: %w", err)
	}
	if string(header[:len(snapshotMagic)]) != snapshotMagic {
		return 0, fmt.Errorf("not a gedis snapshot")
	}
	if version := header[len(snapshotMagic)]; version != snapshotVersion {
		return 0, fmt.Errorf("unsupported snapshot version %d", version)
	}

	loaded := 0
	dbIndex := 0
	expireAt := store.NoExpiry

	for {
		op, err := r.readByte()
		if err != nil {
			return loaded, fmt.Errorf("truncated snapshot: %w", err)
		}

		switch op {
		case opEOF:
			// The checksum trails the terminator and is not covered by itself.
			want := r.crc.Sum64()
			var buf [8]byte
			if _, err := io.ReadFull(r.br, buf[:]); err != nil {
				return loaded, fmt.Errorf("snapshot is missing its checksum")
			}
			if got := binary.LittleEndian.Uint64(buf[:]); got != want {
				return loaded, fmt.Errorf("snapshot checksum mismatch: file says %x, computed %x", got, want)
			}
			return loaded, nil

		case opSelectDB:
			n, err := r.readUvarint()
			if err != nil {
				return loaded, err
			}
			if int(n) >= len(dbs) {
				return loaded, fmt.Errorf("snapshot references database %d but only %d exist", n, len(dbs))
			}
			dbIndex = int(n)

		case opExpiry:
			at, err := r.readInt64()
			if err != nil {
				return loaded, err
			}
			expireAt = at

		default:
			key, value, err := r.readEntry(op)
			if err != nil {
				return loaded, err
			}
			// Drop anything that expired while the server was down.
			if expireAt == store.NoExpiry || expireAt > now {
				dbs[dbIndex].Set(key, value, expireAt)
				loaded++
			}
			expireAt = store.NoExpiry
		}
	}
}

func (r *snapshotReader) readEntry(typ byte) (string, any, error) {
	key, err := r.readString()
	if err != nil {
		return "", nil, err
	}

	switch typ {
	case typeString:
		v, err := r.readBlob()
		return key, v, err

	case typeList:
		n, err := r.readCount()
		if err != nil {
			return "", nil, err
		}
		l := store.NewList()
		for i := 0; i < n; i++ {
			item, err := r.readBlob()
			if err != nil {
				return "", nil, err
			}
			l.PushBack(item)
		}
		return key, l, nil

	case typeHash:
		n, err := r.readCount()
		if err != nil {
			return "", nil, err
		}
		h := store.NewHash()
		for i := 0; i < n; i++ {
			field, err := r.readString()
			if err != nil {
				return "", nil, err
			}
			value, err := r.readBlob()
			if err != nil {
				return "", nil, err
			}
			h.Set(field, value)
		}
		return key, h, nil

	case typeSet:
		n, err := r.readCount()
		if err != nil {
			return "", nil, err
		}
		s := store.NewSet()
		for i := 0; i < n; i++ {
			member, err := r.readString()
			if err != nil {
				return "", nil, err
			}
			s.Add(member)
		}
		return key, s, nil

	case typeZSet:
		n, err := r.readCount()
		if err != nil {
			return "", nil, err
		}
		z := store.NewZSet()
		for i := 0; i < n; i++ {
			member, err := r.readString()
			if err != nil {
				return "", nil, err
			}
			score, err := r.readFloat64()
			if err != nil {
				return "", nil, err
			}
			z.Add(member, score)
		}
		return key, z, nil

	default:
		return "", nil, fmt.Errorf("unknown value type tag %d", typ)
	}
}

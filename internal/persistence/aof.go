// Package persistence gives the keyspace a life beyond one process: an
// append-only log of every mutation, and a binary point-in-time snapshot.
//
// The two answer different questions. The log answers "what happened", so a
// restart can replay it and arrive at the same state; it is durable to within
// one fsync interval but grows without bound. The snapshot answers "what is
// true now", so it loads fast and stays compact, but everything since the last
// one is gone. Redis ships both for exactly this reason, and so does gedis.
package persistence

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// FsyncPolicy controls how often the log is forced to disk.
//
// This is the durability dial, and it is a real trade, not a tuning knob:
// every option loses something.
type FsyncPolicy int

const (
	// FsyncEverySecond flushes on a one-second timer. A crash can lose up to
	// a second of writes. This is the default because it costs almost nothing
	// and bounds the loss.
	FsyncEverySecond FsyncPolicy = iota
	// FsyncAlways fsyncs before replying, so an acknowledged write is on
	// disk. Correct, and roughly an order of magnitude slower, because every
	// write waits on the device.
	FsyncAlways
	// FsyncNever leaves flushing to the operating system. Fastest, and a
	// crash can lose everything still buffered.
	FsyncNever
)

func (p FsyncPolicy) String() string {
	switch p {
	case FsyncAlways:
		return "always"
	case FsyncNever:
		return "no"
	default:
		return "everysec"
	}
}

// ParseFsyncPolicy reads the appendfsync setting.
func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "everysec", "":
		return FsyncEverySecond, nil
	case "always":
		return FsyncAlways, nil
	case "no":
		return FsyncNever, nil
	default:
		return 0, fmt.Errorf("unknown appendfsync policy %q", s)
	}
}

// AOF is the append-only log.
type AOF struct {
	path   string
	policy FsyncPolicy

	mu sync.Mutex
	f  *os.File
	bw *bufio.Writer
	// lastDB is the database the log is currently positioned in, so a SELECT
	// is only emitted when it actually changes.
	lastDB int
	// buffered counts bytes written since the last sync, for INFO.
	written int64
	// rewriting diverts appends into a side buffer while a rewrite is in
	// flight, so writes that arrive mid-rewrite are not lost.
	rewriting bool
	pending   [][]byte

	closed   bool
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// lastError records a failed write so INFO and the logs can report it.
	lastError error
}

// OpenAOF opens or creates the log at path and starts the background syncer.
func OpenAOF(path string, policy FsyncPolicy) (*AOF, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create aof directory: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open aof: %w", err)
	}

	a := &AOF{
		path:   path,
		policy: policy,
		f:      f,
		bw:     bufio.NewWriterSize(f, 64*1024),
		// -1 forces a SELECT before the first command, so a log never
		// depends on which database happened to be current.
		lastDB: -1,
		stop:   make(chan struct{}),
	}
	if policy == FsyncEverySecond {
		a.startSyncer()
	}
	return a, nil
}

// Path returns the file the log is written to.
func (a *AOF) Path() string { return a.path }

// startSyncer flushes and fsyncs on a timer.
//
// Doing this on a background goroutine rather than in the write path is the
// whole point of the everysec policy: clients get their reply as soon as the
// bytes are in the buffer, and durability catches up behind them.
func (a *AOF) startSyncer() {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-ticker.C:
				if err := a.Sync(); err != nil {
					a.mu.Lock()
					a.lastError = err
					a.mu.Unlock()
				}
			}
		}
	}()
}

// Append records one command.
//
// Errors are captured rather than returned. A disk that has stopped accepting
// writes must not turn every client command into an error reply -- the server
// keeps serving from memory, reports the failure through INFO, and logs it.
func (a *AOF) Append(dbIndex int, args [][]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}

	buf := encodeCommand(dbIndex, a.lastDB, args)
	a.lastDB = dbIndex

	if a.rewriting {
		// A rewrite is building a fresh file from a snapshot of the data.
		// Anything written meanwhile has to be replayed on top of it.
		a.pending = append(a.pending, buf)
	}

	if _, err := a.bw.Write(buf); err != nil {
		a.lastError = err
		return
	}
	a.written += int64(len(buf))

	if a.policy == FsyncAlways {
		if err := a.syncLocked(); err != nil {
			a.lastError = err
		}
	}
}

// encodeCommand renders a command as RESP, preceded by a SELECT when the
// target database differs from the log's current position.
func encodeCommand(dbIndex, lastDB int, args [][]byte) []byte {
	var buf []byte
	if dbIndex != lastDB {
		buf = appendCommand(buf, [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(dbIndex))})
	}
	return appendCommand(buf, args)
}

// appendCommand writes one multibulk command.
//
// The log is a plain stream of RESP commands, which means the same reader the
// server uses for clients can replay it, and a human can read it with `cat`.
func appendCommand(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// Sync flushes buffered bytes and forces them to the device.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	return a.syncLocked()
}

func (a *AOF) syncLocked() error {
	if err := a.bw.Flush(); err != nil {
		return err
	}
	if a.policy == FsyncNever {
		// Leave it to the operating system, which is what "no" means.
		return nil
	}
	return a.f.Sync()
}

// Size returns the log's size on disk.
func (a *AOF) Size() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0
	}
	_ = a.bw.Flush()
	info, err := a.f.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// LastError returns the most recent write or sync failure, if any.
func (a *AOF) LastError() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastError
}

// Close stops the syncer and flushes everything still buffered.
func (a *AOF) Close() error {
	a.stopOnce.Do(func() { close(a.stop) })
	a.wg.Wait()

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	if err := a.bw.Flush(); err != nil {
		_ = a.f.Close()
		return err
	}
	if err := a.f.Sync(); err != nil {
		_ = a.f.Close()
		return err
	}
	return a.f.Close()
}

// --- rewriting ---

// BeginRewrite starts capturing concurrent writes and returns a temporary path
// to build the new log at.
//
// The rewrite works from a point-in-time copy of the data, which takes time to
// serialise. Commands that arrive during that window belong in the new file
// too, so they are buffered here and appended once the fresh contents are in
// place.
func (a *AOF) BeginRewrite() (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return "", fmt.Errorf("aof is closed")
	}
	if a.rewriting {
		return "", fmt.Errorf("background append only file rewriting already in progress")
	}
	a.rewriting = true
	a.pending = nil
	return a.path + ".rewrite", nil
}

// FinishRewrite swaps a completed rewrite into place.
//
// The old file is replaced by a rename, which is atomic on both POSIX and
// Windows: a crash mid-swap leaves either the whole old log or the whole new
// one, never a half-written mixture.
func (a *AOF) FinishRewrite(tempPath string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.rewriting {
		return fmt.Errorf("no rewrite in progress")
	}
	defer func() {
		a.rewriting = false
		a.pending = nil
	}()

	// Append everything that happened while the snapshot was being written.
	temp, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	for _, buf := range a.pending {
		if _, err := temp.Write(buf); err != nil {
			_ = temp.Close()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}

	// Release the old handle before renaming: Windows refuses to replace a
	// file that is still open.
	if err := a.bw.Flush(); err != nil {
		return err
	}
	if err := a.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, a.path); err != nil {
		// The rename failed, so reopen the original and carry on logging to
		// it rather than leaving the server with no log at all.
		if f, reopenErr := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); reopenErr == nil {
			a.f = f
			a.bw = bufio.NewWriterSize(f, 64*1024)
		}
		return err
	}

	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	a.f = f
	a.bw = bufio.NewWriterSize(f, 64*1024)
	// The rewritten file carries its own SELECT statements, and the appended
	// tail was encoded against the old position, so reset the tracker.
	a.lastDB = -1
	return nil
}

// AbortRewrite abandons a rewrite that failed partway.
func (a *AOF) AbortRewrite(tempPath string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rewriting = false
	a.pending = nil
	_ = os.Remove(tempPath)
}

// RewriteWriter builds a new log file from a snapshot of the keyspace.
type RewriteWriter struct {
	f      *os.File
	bw     *bufio.Writer
	lastDB int
}

// NewRewriteWriter creates the temporary file a rewrite is assembled in.
func NewRewriteWriter(path string) (*RewriteWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &RewriteWriter{f: f, bw: bufio.NewWriterSize(f, 256*1024), lastDB: -1}, nil
}

// WriteCommand appends one reconstruction command.
func (w *RewriteWriter) WriteCommand(dbIndex int, args [][]byte) error {
	buf := encodeCommand(dbIndex, w.lastDB, args)
	w.lastDB = dbIndex
	_, err := w.bw.Write(buf)
	return err
}

// Close flushes and closes the temporary file.
func (w *RewriteWriter) Close() error {
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// --- loading ---

// LoadAOF replays a log, handing each command to apply.
//
// A truncated tail is tolerated rather than treated as corruption: it is the
// expected shape of a crash partway through a write, and refusing to start
// would turn a recoverable incident into an outage. Everything up to the
// truncation is applied, and the caller is told how much was read.
func LoadAOF(path string, apply func(dbIndex int, args [][]byte) error) (commands int, truncated bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 256*1024)
	dbIndex := 0

	for {
		args, readErr := readCommand(r)
		if readErr == io.EOF {
			return commands, truncated, nil
		}
		if readErr != nil {
			// Anything unreadable from here on is the crash tail.
			return commands, true, nil
		}
		if len(args) == 0 {
			continue
		}

		// SELECT is bookkeeping for the log itself, not a command to replay.
		if len(args) == 2 && equalFoldASCII(args[0], "select") {
			n, convErr := strconv.Atoi(string(args[1]))
			if convErr != nil {
				return commands, true, nil
			}
			dbIndex = n
			continue
		}

		if applyErr := apply(dbIndex, args); applyErr != nil {
			return commands, false, fmt.Errorf("replaying command %d (%s): %w",
				commands+1, string(args[0]), applyErr)
		}
		commands++
	}
}

// readCommand reads one multibulk command from the log.
func readCommand(r *bufio.Reader) ([][]byte, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("expected '*', got %q", prefix)
	}

	n, err := readNumberLine(r)
	if err != nil {
		return nil, err
	}
	if n <= 0 || n > 1024*1024 {
		return nil, fmt.Errorf("invalid multibulk length %d", n)
	}

	args := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != '$' {
			return nil, fmt.Errorf("expected '$', got %q", b)
		}
		size, err := readNumberLine(r)
		if err != nil {
			return nil, err
		}
		if size < 0 || size > 512*1024*1024 {
			return nil, fmt.Errorf("invalid bulk length %d", size)
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, fmt.Errorf("bulk not terminated by CRLF")
		}
		args = append(args, buf[:size])
	}
	return args, nil
}

func readNumberLine(r *bufio.Reader) (int64, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return 0, fmt.Errorf("unterminated line")
	}
	return strconv.ParseInt(line[:len(line)-2], 10, 64)
}

// equalFoldASCII compares without allocating, for the SELECT check on the
// replay hot path.
func equalFoldASCII(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

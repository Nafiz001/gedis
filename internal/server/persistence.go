package server

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Nafiz001/gedis/internal/persistence"
	"github.com/Nafiz001/gedis/internal/resp"
	"github.com/Nafiz001/gedis/internal/store"
)

// aofPath and snapshotPath resolve the configured filenames against Dir.
func (s *Server) aofPath() string {
	return filepath.Join(s.cfg.Dir, s.cfg.AppendFilename)
}

func (s *Server) snapshotPath() string {
	return filepath.Join(s.cfg.Dir, s.cfg.SnapshotFilename)
}

// internalClient executes commands with no socket behind them, for replaying
// the log at startup and for applying a master's stream on a replica.
//
// It is a real client running the real dispatch path, which is the point: a
// replayed SET goes through exactly the code a networked SET does, so replay
// can never drift from live behaviour. Its replies go to a buffer that is
// inspected for errors and then discarded.
type internalClient struct {
	c   *client
	buf *bytes.Buffer
}

func (s *Server) newInternalClient() *internalClient {
	buf := &bytes.Buffer{}
	c := &client{
		srv:           s,
		w:             resp.NewWriter(buf),
		db:            s.dbs[0],
		dbIndex:       0,
		authenticated: true,
		internal:      true,
	}
	return &internalClient{c: c, buf: buf}
}

// exec runs one command against the given database and reports an error reply
// as a Go error.
func (ic *internalClient) exec(dbIndex int, args [][]byte) error {
	if dbIndex != ic.c.dbIndex {
		if err := ic.c.selectDB(dbIndex); err != nil {
			return err
		}
	}
	ic.buf.Reset()
	ic.c.srv.dispatch(ic.c, args)
	_ = ic.c.w.Flush()

	// A leading '-' is the RESP error marker.
	if b := ic.buf.Bytes(); len(b) > 0 && b[0] == '-' {
		line := ic.buf.String()
		if i := strings.IndexByte(line, '\r'); i >= 0 {
			line = line[1:i]
		}
		return fmt.Errorf("%s", line)
	}
	return nil
}

// loadFromDisk restores the keyspace at startup.
//
// The append-only log wins when it is enabled, because it is the more recent
// and more complete record: the snapshot only reaches the last save point,
// while the log reaches the last fsync. Redis makes the same choice.
func (s *Server) loadFromDisk() error {
	s.loading.Store(true)
	defer s.loading.Store(false)

	ic := s.newInternalClient()
	start := time.Now()

	if s.cfg.AppendOnly {
		count, truncated, err := persistence.LoadAOF(s.aofPath(), func(dbIndex int, args [][]byte) error {
			return ic.exec(dbIndex, args)
		})
		if err != nil {
			return fmt.Errorf("load append-only file: %w", err)
		}
		if count > 0 || truncated {
			s.log.Info("restored from the append-only file",
				"commands", count,
				"truncated_tail", truncated,
				"took", time.Since(start).Round(time.Millisecond),
			)
		}
		if truncated {
			// Expected after a crash mid-write, not a reason to refuse to
			// start: everything before the tear is intact and already applied.
			s.log.Warn("the append-only file ended mid-command; the incomplete tail was discarded")
		}
		return nil
	}

	loaded, err := persistence.LoadSnapshot(s.snapshotPath(), s.dbs, s.dbs[0].Now())
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	if loaded > 0 {
		s.log.Info("restored from snapshot",
			"keys", loaded,
			"took", time.Since(start).Round(time.Millisecond),
		)
	}
	return nil
}

// openAOF starts the append-only log, after any restore has finished.
func (s *Server) openAOF() error {
	if !s.cfg.AppendOnly {
		return nil
	}
	aof, err := persistence.OpenAOF(s.aofPath(), s.cfg.AppendFsync)
	if err != nil {
		return err
	}
	s.aof = aof
	s.log.Info("append-only logging enabled",
		"path", aof.Path(), "fsync", s.cfg.AppendFsync.String())
	return nil
}

// startSavePoints watches the change counter and snapshots on schedule.
func (s *Server) startSavePoints() {
	if len(s.cfg.SavePoints) == 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopSave:
				return
			case <-ticker.C:
				s.maybeSave()
			}
		}
	}()
}

// maybeSave snapshots if any configured save point is satisfied.
//
// The points are deliberately overlapping thresholds rather than one interval:
// a busy server should checkpoint often, an idle one rarely, and a single
// timer cannot express both.
func (s *Server) maybeSave() {
	dirty := s.dirty.Load()
	if dirty == 0 {
		return
	}
	elapsed := time.Since(time.Unix(s.lastSave.Load(), 0))

	for _, point := range s.cfg.SavePoints {
		if dirty >= int64(point.Changes) && elapsed >= point.After {
			s.backgroundSave()
			return
		}
	}
}

// save writes a snapshot and resets the change counter.
func (s *Server) save() (int, error) {
	// The counter is captured before the write, not after: writes that land
	// during the save are not represented in it and must still count toward
	// the next one.
	before := s.dirty.Load()

	keys, err := persistence.SaveSnapshot(s.snapshotPath(), s.dbs)
	if err != nil {
		return 0, err
	}
	s.dirty.Add(-before)
	s.lastSave.Store(time.Now().Unix())
	return keys, nil
}

// backgroundSave snapshots on its own goroutine, refusing to start a second
// one while the first is still running.
func (s *Server) backgroundSave() {
	if !s.saving.CompareAndSwap(false, true) {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.saving.Store(false)

		start := time.Now()
		keys, err := s.save()
		if err != nil {
			s.log.Error("background snapshot failed", "err", err)
			return
		}
		s.log.Info("background snapshot complete",
			"keys", keys, "took", time.Since(start).Round(time.Millisecond))
	}()
}

// rewriteAOF rebuilds the log from current state.
//
// The log grows without bound: a counter incremented a million times leaves a
// million lines that reconstruct one integer. A rewrite replaces the history
// with the shortest set of commands that produces the same data, so the file
// tracks how much data there is rather than how much has happened.
func (s *Server) rewriteAOF() error {
	if s.aof == nil {
		return fmt.Errorf("append-only logging is disabled")
	}
	if !s.rewriting.CompareAndSwap(false, true) {
		return fmt.Errorf("background append only file rewriting already in progress")
	}
	defer s.rewriting.Store(false)

	tempPath, err := s.aof.BeginRewrite()
	if err != nil {
		return err
	}

	w, err := persistence.NewRewriteWriter(tempPath)
	if err != nil {
		s.aof.AbortRewrite(tempPath)
		return err
	}

	if err := s.writeRewriteBody(w); err != nil {
		_ = w.Close()
		s.aof.AbortRewrite(tempPath)
		return err
	}
	if err := w.Close(); err != nil {
		s.aof.AbortRewrite(tempPath)
		return err
	}
	return s.aof.FinishRewrite(tempPath)
}

// writeRewriteBody emits the commands that rebuild every database.
func (s *Server) writeRewriteBody(w *persistence.RewriteWriter) error {
	for dbIndex, db := range s.dbs {
		if db.Len() == 0 {
			continue
		}
		for shard := 0; shard < db.ShardCount(); shard++ {
			var shardErr error
			db.ForEachInShard(shard, func(key string, value any, expireAt int64) {
				if shardErr != nil {
					return
				}
				for _, cmd := range rebuildCommands(key, value, expireAt) {
					if err := w.WriteCommand(dbIndex, cmd); err != nil {
						shardErr = err
						return
					}
				}
			})
			if shardErr != nil {
				return shardErr
			}
		}
	}
	return nil
}

// rebuildCommandBatch bounds how many elements go into one reconstruction
// command, so a million-member set does not become a single command that no
// reader can buffer.
const rebuildCommandBatch = 128

// rebuildCommands returns the commands that recreate one key.
//
// Deadlines are emitted as PEXPIREAT rather than EXPIRE: the absolute form
// survives replay at any later time, while a relative one would silently renew
// the key's life on every restart.
func rebuildCommands(key string, value any, expireAt int64) [][][]byte {
	var out [][][]byte

	switch v := value.(type) {
	case []byte:
		out = append(out, buildCmd("SET").str(key).bytes(v))

	case *store.List:
		items := v.All()
		for i := 0; i < len(items); i += rebuildCommandBatch {
			end := min(i+rebuildCommandBatch, len(items))
			cmd := buildCmd("RPUSH").str(key)
			for _, item := range items[i:end] {
				cmd = cmd.bytes(item)
			}
			out = append(out, cmd)
		}

	case *store.Hash:
		pairs := v.All()
		for i := 0; i < len(pairs); i += rebuildCommandBatch {
			end := min(i+rebuildCommandBatch, len(pairs))
			cmd := buildCmd("HSET").str(key)
			for _, p := range pairs[i:end] {
				cmd = cmd.str(p.Field).bytes(p.Value)
			}
			out = append(out, cmd)
		}

	case *store.Set:
		members := v.Members()
		for i := 0; i < len(members); i += rebuildCommandBatch {
			end := min(i+rebuildCommandBatch, len(members))
			cmd := buildCmd("SADD").str(key)
			for _, m := range members[i:end] {
				cmd = cmd.str(m)
			}
			out = append(out, cmd)
		}

	case *store.ZSet:
		members := v.RangeByRank(0, -1, false)
		for i := 0; i < len(members); i += rebuildCommandBatch {
			end := min(i+rebuildCommandBatch, len(members))
			cmd := buildCmd("ZADD").str(key)
			for _, m := range members[i:end] {
				cmd = cmd.str(formatScore(m.Score)).str(m.Member)
			}
			out = append(out, cmd)
		}

	default:
		return nil
	}

	if expireAt != store.NoExpiry {
		out = append(out, buildCmd("PEXPIREAT").str(key).int(expireAt))
	}
	return out
}

// --- commands ---

func cmdSave(c *client, args [][]byte) {
	start := time.Now()
	keys, err := c.srv.save()
	if err != nil {
		c.replyErrorf("ERR %s", err.Error())
		return
	}
	c.srv.log.Info("snapshot written",
		"keys", keys, "took", time.Since(start).Round(time.Millisecond))
	c.replyOK()
}

func cmdBGSave(c *client, args [][]byte) {
	if c.srv.saving.Load() {
		c.replyError("ERR Background save already in progress")
		return
	}
	c.srv.backgroundSave()
	c.replySimple("Background saving started")
}

func cmdBGRewriteAOF(c *client, args [][]byte) {
	if c.srv.aof == nil {
		c.replyError("ERR Append only logging is not enabled")
		return
	}
	if c.srv.rewriting.Load() {
		c.replyError("ERR Background append only file rewriting already in progress")
		return
	}

	srv := c.srv
	srv.wg.Add(1)
	go func() {
		defer srv.wg.Done()
		start := time.Now()
		if err := srv.rewriteAOF(); err != nil {
			srv.log.Error("append-only rewrite failed", "err", err)
			return
		}
		srv.log.Info("append-only rewrite complete",
			"size", srv.aof.Size(), "took", time.Since(start).Round(time.Millisecond))
	}()
	c.replySimple("Background append only file rewriting started")
}

func cmdLastSave(c *client, args [][]byte) {
	c.replyInt(c.srv.lastSave.Load())
}

// persistenceInfo renders the INFO persistence section.
func (s *Server) persistenceInfo() string {
	var b strings.Builder
	b.WriteString("# Persistence\r\n")
	fmt.Fprintf(&b, "loading:%d\r\n", boolToInt(s.loading.Load()))
	fmt.Fprintf(&b, "rdb_changes_since_last_save:%d\r\n", s.dirty.Load())
	fmt.Fprintf(&b, "rdb_bgsave_in_progress:%d\r\n", boolToInt(s.saving.Load()))
	fmt.Fprintf(&b, "rdb_last_save_time:%d\r\n", s.lastSave.Load())
	fmt.Fprintf(&b, "aof_enabled:%d\r\n", boolToInt(s.aof != nil))
	fmt.Fprintf(&b, "aof_rewrite_in_progress:%d\r\n", boolToInt(s.rewriting.Load()))
	if s.aof != nil {
		fmt.Fprintf(&b, "aof_current_size:%d\r\n", s.aof.Size())
		status := "ok"
		if err := s.aof.LastError(); err != nil {
			status = err.Error()
		}
		fmt.Fprintf(&b, "aof_last_write_status:%s\r\n", status)
	}
	b.WriteString("\r\n")
	return b.String()
}

// ParseSavePoints reads the "seconds:changes,seconds:changes" flag form.
func ParseSavePoints(spec string) ([]SavePoint, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" || spec == "off" {
		return nil, nil
	}

	var points []SavePoint
	for _, part := range strings.Split(spec, ",") {
		fields := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(fields) != 2 {
			return nil, fmt.Errorf("save point %q must look like seconds:changes", part)
		}
		seconds, err := strconv.Atoi(fields[0])
		if err != nil || seconds <= 0 {
			return nil, fmt.Errorf("save point %q has an invalid interval", part)
		}
		changes, err := strconv.Atoi(fields[1])
		if err != nil || changes <= 0 {
			return nil, fmt.Errorf("save point %q has an invalid change count", part)
		}
		points = append(points, SavePoint{
			After:   time.Duration(seconds) * time.Second,
			Changes: changes,
		})
	}
	return points, nil
}

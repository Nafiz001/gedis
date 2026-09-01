package server

import "strconv"

// Write propagation: the single stream of mutations that both the append-only
// file and any connected replicas consume.
//
// A command is propagated verbatim unless a handler says otherwise, because
// replaying the original is correct whenever the command is deterministic. Two
// kinds of command are not, and each is rewritten before it leaves:
//
//   - Relative deadlines. "SET k v EX 60" replayed an hour later would extend
//     the key's life by another minute. Handlers rewrite these to the absolute
//     PXAT/PEXPIREAT form, so a replay reconstructs the same expiry instant.
//   - Random effects. SPOP chooses members at random, so replaying it would
//     remove different ones. It is rewritten to the SREM it turned out to be.
//
// Commands are propagated even when they changed nothing -- a DEL of a missing
// key still appends a line. Redis avoids that with a dirty counter threaded
// through every handler; here the AOF is marginally larger in exchange for the
// rule staying simple enough to be obviously correct.

// cmdBuilder assembles a command for propagation.
type cmdBuilder [][]byte

func buildCmd(name string) cmdBuilder {
	return cmdBuilder{[]byte(name)}
}

func (b cmdBuilder) str(s string) cmdBuilder   { return append(b, []byte(s)) }
func (b cmdBuilder) bytes(v []byte) cmdBuilder { return append(b, append([]byte(nil), v...)) }

func (b cmdBuilder) int(n int64) cmdBuilder {
	return append(b, strconv.AppendInt(nil, n, 10))
}

// rewriteAs replaces what this command contributes to the write stream.
func (c *client) rewriteAs(b cmdBuilder) {
	c.propagateOverride = b
}

// suppressPropagation keeps this command out of the write stream entirely,
// which handlers use when a conditional write did not fire.
func (c *client) suppressPropagation() {
	c.noPropagate = true
}

// propagate hands one mutation to every consumer of the write stream.
//
// It runs after the handler, on the client's own goroutine, so the ordering
// clients observe is the ordering that reaches the AOF and the replicas.
func (s *Server) propagate(dbIndex int, args [][]byte) {
	if s.aof != nil {
		s.aof.Append(dbIndex, args)
	}
	s.replicas.propagate(dbIndex, args)
}

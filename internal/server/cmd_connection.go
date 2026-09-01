package server

import (
	"crypto/subtle"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

func cmdPing(c *client, args [][]byte) {
	// In subscribe mode every frame the client receives is an array, so PING
	// answers in that shape too rather than with a simple string -- otherwise
	// a client demultiplexing pushes would choke on the odd one out. Verified
	// against Redis 7.
	if c.inSubscribeMode() {
		payload := ""
		if len(args) == 2 {
			payload = string(args[1])
		}
		c.replyArrayLen(2)
		c.replyBulkString("pong")
		c.replyBulkString(payload)
		return
	}

	switch len(args) {
	case 1:
		c.replySimple("PONG")
	case 2:
		// PING with a payload echoes it back, which is how clients measure
		// round-trip latency without a second command.
		c.replyBulk(args[1])
	default:
		c.replyWrongArity("ping")
	}
}

func cmdEcho(c *client, args [][]byte) {
	c.replyBulk(args[1])
}

func cmdQuit(c *client, args [][]byte) {
	c.replyOK()
	c.closeAfterReply = true
}

func cmdSelect(c *client, args [][]byte) {
	n, err := strconv.Atoi(string(args[1]))
	if err != nil {
		c.replyError("ERR value is not an integer or out of range")
		return
	}
	if err := c.selectDB(n); err != nil {
		c.replyErrorf("ERR %s", err.Error())
		return
	}
	c.replyOK()
}

func cmdAuth(c *client, args [][]byte) {
	if c.srv.cfg.Password == "" {
		c.replyError("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")
		return
	}

	// AUTH takes either a password or a username/password pair; gedis has a
	// single implicit "default" user, so only that username is accepted.
	var password string
	switch len(args) {
	case 2:
		password = string(args[1])
	case 3:
		if !strings.EqualFold(string(args[1]), "default") {
			c.replyError("WRONGPASS invalid username-password pair or user is disabled.")
			return
		}
		password = string(args[2])
	default:
		c.replyWrongArity("auth")
		return
	}

	// Constant-time comparison: a byte-by-byte compare leaks the length of the
	// matching prefix through timing, which is enough to recover a password.
	if subtle.ConstantTimeCompare([]byte(password), []byte(c.srv.cfg.Password)) != 1 {
		c.replyError("WRONGPASS invalid username-password pair or user is disabled.")
		return
	}
	c.authenticated = true
	c.replyOK()
}

// cmdHello implements the handshake modern clients use to negotiate a protocol
// version and learn about the server.
func cmdHello(c *client, args [][]byte) {
	i := 1
	if i < len(args) {
		ver, err := strconv.Atoi(string(args[i]))
		if err != nil {
			c.replyError("NOPROTO unsupported protocol version")
			return
		}
		if ver != 2 {
			// gedis speaks RESP2 only. Saying so explicitly lets a RESP3
			// client fall back instead of misreading replies.
			c.replyError("NOPROTO unsupported protocol version")
			return
		}
		i++
	}

	for ; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "auth":
			if i+2 >= len(args) {
				c.replySyntaxError()
				return
			}
			cmdAuth(c, [][]byte{[]byte("auth"), args[i+1], args[i+2]})
			// cmdAuth already replied; HELLO cannot also reply on this path.
			return
		case "setname":
			if i+1 >= len(args) {
				c.replySyntaxError()
				return
			}
			i++
			c.name = string(args[i])
		default:
			c.replySyntaxError()
			return
		}
	}

	if !c.authenticated {
		c.replyError("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
		return
	}

	// In RESP2 a map is sent as a flat array of alternating keys and values.
	c.replyArrayLen(14)
	c.replyBulkString("server")
	c.replyBulkString("gedis")
	c.replyBulkString("version")
	c.replyBulkString(Version)
	c.replyBulkString("proto")
	c.replyInt(2)
	c.replyBulkString("id")
	c.replyInt(int64(c.id))
	c.replyBulkString("mode")
	c.replyBulkString("standalone")
	c.replyBulkString("role")
	c.replyBulkString("master")
	c.replyBulkString("modules")
	c.replyEmptyArray()
}

func cmdReset(c *client, args [][]byte) {
	_ = c.selectDB(0)
	c.name = ""
	c.authenticated = c.srv.cfg.Password == ""
	c.replySimple("RESET")
}

func cmdClient(c *client, args [][]byte) {
	switch strings.ToLower(string(args[1])) {
	case "id":
		c.replyInt(int64(c.id))

	case "getname":
		if c.name == "" {
			c.replyNull()
			return
		}
		c.replyBulkString(c.name)

	case "setname":
		if len(args) != 3 {
			c.replyWrongArity("client|setname")
			return
		}
		name := string(args[2])
		// Spaces and newlines would corrupt the single-line-per-client format
		// that CLIENT LIST produces.
		if strings.ContainsAny(name, " \n\r") {
			c.replyError("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
		c.name = name
		c.replyOK()

	case "setinfo":
		// Clients announce their library name and version here. Nothing uses
		// it yet, but rejecting it would break connection setup for clients
		// that send it unconditionally.
		if len(args) != 4 {
			c.replyWrongArity("client|setinfo")
			return
		}
		c.replyOK()

	case "list":
		clients := c.srv.snapshotClients()
		sort.Slice(clients, func(i, j int) bool { return clients[i].id < clients[j].id })
		var b strings.Builder
		for _, cl := range clients {
			b.WriteString(cl.infoLine())
			b.WriteByte('\n')
		}
		c.replyBulkString(b.String())

	case "info":
		c.replyBulkString(c.infoLine())

	default:
		c.replyErrorf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try CLIENT HELP.",
			string(args[1]))
	}
}

// infoLine renders one client in the field=value format CLIENT LIST uses.
func (c *client) infoLine() string {
	age := int64(time.Since(c.created).Seconds())
	return fmt.Sprintf("id=%d addr=%s name=%s age=%d db=%d cmd=%s",
		c.id, c.addr, c.name, age, c.dbIndex, c.lastCommand)
}

// cmdCommand implements COMMAND and its subcommands.
//
// redis-cli issues COMMAND DOCS on connect to build its help and tab
// completion. Answering it -- even with an empty reply -- is what stops the
// official CLI from printing an error every time it attaches.
func cmdCommand(c *client, args [][]byte) {
	if len(args) == 1 {
		names := sortedCommandNames()
		c.replyArrayLen(len(names))
		for _, name := range names {
			writeCommandInfo(c, commands[name])
		}
		return
	}

	switch strings.ToLower(string(args[1])) {
	case "count":
		c.replyInt(int64(len(commands)))

	case "docs":
		c.replyEmptyArray()

	case "info":
		if len(args) == 2 {
			names := sortedCommandNames()
			c.replyArrayLen(len(names))
			for _, name := range names {
				writeCommandInfo(c, commands[name])
			}
			return
		}
		c.replyArrayLen(len(args) - 2)
		for _, raw := range args[2:] {
			spec, ok := commands[strings.ToLower(string(raw))]
			if !ok {
				c.replyNullArray()
				continue
			}
			writeCommandInfo(c, spec)
		}

	default:
		c.replyErrorf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try COMMAND HELP.",
			string(args[1]))
	}
}

func sortedCommandNames() []string {
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// writeCommandInfo emits the six-element description clients expect: name,
// arity, flags, and the first/last/step key positions.
func writeCommandInfo(c *client, spec commandSpec) {
	c.replyArrayLen(6)
	c.replyBulkString(spec.name)
	c.replyInt(int64(spec.arity))

	flags := []string{"fast"}
	if spec.write {
		flags = []string{"write", "denyoom"}
	} else if spec.arity != 1 {
		flags = []string{"readonly", "fast"}
	}
	c.replyArrayLen(len(flags))
	for _, f := range flags {
		c.replySimple(f)
	}

	// Key positions: commands that take a key take it first. Nothing here
	// consumes these yet, but clients that shard by key hash read them.
	first, last, step := 0, 0, 0
	if spec.arity != 1 && spec.name != "select" && spec.name != "auth" {
		first, last, step = 1, 1, 1
	}
	c.replyInt(int64(first))
	c.replyInt(int64(last))
	c.replyInt(int64(step))
}

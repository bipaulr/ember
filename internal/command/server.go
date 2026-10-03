package command

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"ember/internal/glob"
)

func registerServer(t *Table) {
	t.add(Spec{Name: "info", Arity: -1, Flags: []string{FlagReadonly}, Handler: cmdInfo})
	t.add(Spec{Name: "command", Arity: -1, Flags: []string{FlagReadonly}, Handler: cmdCommand})
	t.add(Spec{Name: "config", Arity: -2, Flags: []string{FlagAdmin}, Handler: cmdConfig})
	t.add(Spec{Name: "client", Arity: -2, Flags: []string{FlagAdmin}, Handler: cmdClient})
	t.add(Spec{Name: "hello", Arity: -1, Flags: []string{FlagFast}, Handler: cmdHello})
}

var infoSections = []string{"server", "clients", "stats", "persistence", "keyspace"}

func cmdInfo(c *Ctx) {
	want := infoSections
	if len(c.Args) > 1 {
		want = nil
		for _, a := range c.Args[1:] {
			s := strings.ToLower(string(a))
			if s == "all" || s == "default" || s == "everything" {
				want = infoSections
				break
			}
			want = append(want, s)
		}
	}
	var sb strings.Builder
	for _, s := range want {
		if sb.Len() > 0 {
			sb.WriteString("\r\n")
		}
		writeInfoSection(&sb, c, s)
	}
	c.W.WriteBulkString(sb.String())
}

func writeInfoSection(sb *strings.Builder, c *Ctx, section string) {
	env := c.Env
	line := func(k string, v any) { fmt.Fprintf(sb, "%s:%v\r\n", k, v) }
	switch section {
	case "server":
		sb.WriteString("# Server\r\n")
		up := time.Since(env.Start)
		line("redis_version", "7.2.0-compat")
		line("ember_version", Version)
		line("redis_mode", "standalone")
		line("os", runtime.GOOS+" "+runtime.GOARCH)
		line("arch_bits", 32<<(^uint(0)>>63))
		line("go_version", runtime.Version())
		line("process_id", os.Getpid())
		line("tcp_port", env.Port)
		line("uptime_in_seconds", int64(up.Seconds()))
		line("uptime_in_days", int64(up.Hours()/24))
		line("shards", env.Store.NumShards())
	case "clients":
		sb.WriteString("# Clients\r\n")
		line("connected_clients", env.Stats().Clients)
		line("maxclients", env.MaxClients)
	case "stats":
		st := env.Stats()
		sb.WriteString("# Stats\r\n")
		line("total_connections_received", env.TotalConns.Load())
		line("total_commands_processed", st.Commands)
		line("rejected_connections", env.Rejected.Load())
		line("expired_keys", env.Store.ExpiredKeys())
		line("keyspace_hits", st.Hits)
		line("keyspace_misses", st.Misses)
		line("pubsub_channels", env.Hub.NumChannels())
		line("pubsub_patterns", env.Hub.NumPat())
		line("pubsub_messages_published", env.Hub.Published.Load())
		line("pubsub_slow_subscribers_disconnected", env.Hub.Overflows.Load())
	case "persistence":
		sb.WriteString("# Persistence\r\n")
		writePersistenceInfo(sb, env)
	case "keyspace":
		sb.WriteString("# Keyspace\r\n")
		if n := env.Store.Len(); n > 0 {
			fmt.Fprintf(sb, "db0:keys=%d,expires=%d,avg_ttl=0\r\n", n, env.Store.TTLKeys())
		}
	}
}

func cmdCommand(c *Ctx) {
	t := c.Env.Table
	if len(c.Args) == 1 {
		specs := sortedSpecs(t)
		c.W.WriteArrayHeader(len(specs))
		for _, s := range specs {
			writeCommandInfo(c, s)
		}
		return
	}
	switch upper(c.Args[1]) {
	case "COUNT":
		c.W.WriteInt(int64(len(t.m)))
	case "DOCS":
		// redis-cli asks for docs to build its hints; an empty map is valid.
		c.W.WriteArrayHeader(0)
	case "INFO":
		c.W.WriteArrayHeader(len(c.Args) - 2)
		for _, n := range c.Args[2:] {
			if s := t.Lookup(n); s != nil {
				writeCommandInfo(c, s)
			} else {
				c.W.WriteNullArray()
			}
		}
	case "LIST":
		specs := sortedSpecs(t)
		c.W.WriteArrayHeader(len(specs))
		for _, s := range specs {
			c.W.WriteBulkString(s.Name)
		}
	default:
		c.W.WriteError("ERR unknown subcommand '" + sanitize(c.Args[1]) + "'. Try COMMAND HELP.")
	}
}

func sortedSpecs(t *Table) []*Spec {
	specs := t.Specs()
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

func writeCommandInfo(c *Ctx, s *Spec) {
	c.W.WriteArrayHeader(6)
	c.W.WriteBulkString(s.Name)
	c.W.WriteInt(int64(s.Arity))
	c.W.WriteArrayHeader(len(s.Flags))
	for _, f := range s.Flags {
		c.W.WriteSimple(f)
	}
	c.W.WriteInt(int64(s.FirstKey))
	c.W.WriteInt(int64(s.LastKey))
	c.W.WriteInt(int64(s.Step))
}

func cmdConfig(c *Ctx) {
	switch upper(c.Args[1]) {
	case "GET":
		if len(c.Args) < 3 {
			c.W.WriteError("ERR wrong number of arguments for 'config|get' command")
			return
		}
		vals := c.Env.configValues()
		var keys []string
		for k := range vals {
			for _, p := range c.Args[2:] {
				if glob.Match(strings.ToLower(string(p)), k) {
					keys = append(keys, k)
					break
				}
			}
		}
		sort.Strings(keys)
		c.W.WriteArrayHeader(len(keys) * 2)
		for _, k := range keys {
			c.W.WriteBulkString(k)
			c.W.WriteBulkString(vals[k])
		}
	default:
		c.W.WriteError("ERR unsupported CONFIG subcommand; only CONFIG GET is implemented")
	}
}

// configValues is the read-only view served by CONFIG GET.
func (e *Env) configValues() map[string]string {
	return map[string]string{
		"port":       strconv.Itoa(e.Port),
		"maxclients": strconv.Itoa(e.MaxClients),
		"databases":  "1",
		"save":       "",
		"appendonly": e.appendOnly(),
	}
}

func cmdClient(c *Ctx) {
	switch upper(c.Args[1]) {
	case "ID":
		c.W.WriteInt(c.Client.ID)
	case "SETNAME":
		if len(c.Args) != 3 {
			c.W.WriteError("ERR wrong number of arguments for 'client|setname' command")
			return
		}
		c.Client.Name = string(c.Args[2])
		c.W.WriteSimple("OK")
	case "GETNAME":
		if c.Client.Name == "" {
			c.W.WriteNull()
		} else {
			c.W.WriteBulkString(c.Client.Name)
		}
	case "SETINFO":
		c.W.WriteSimple("OK")
	default:
		c.W.WriteError("ERR unknown subcommand '" + sanitize(c.Args[1]) + "'")
	}
}

func cmdHello(c *Ctx) {
	if len(c.Args) > 1 {
		v, ok := parseInt(c.Args[1])
		if !ok {
			c.W.WriteError("ERR Protocol version is not an integer or out of range")
			return
		}
		if v != 2 {
			c.W.WriteError("NOPROTO sorry, this protocol version is not supported")
			return
		}
	}
	c.W.WriteArrayHeader(12)
	c.W.WriteBulkString("server")
	c.W.WriteBulkString("ember")
	c.W.WriteBulkString("version")
	c.W.WriteBulkString(Version)
	c.W.WriteBulkString("proto")
	c.W.WriteInt(2)
	c.W.WriteBulkString("id")
	c.W.WriteInt(c.Client.ID)
	c.W.WriteBulkString("mode")
	c.W.WriteBulkString("standalone")
	c.W.WriteBulkString("role")
	c.W.WriteBulkString("master")
}

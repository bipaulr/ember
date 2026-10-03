package command

import (
	"errors"
	"fmt"
	"strings"
)

// PersistInfo describes the append-only file for INFO.
type PersistInfo struct {
	Path              string
	Policy            string
	Size              int64
	BaseSize          int64
	RewriteInProgress bool
	Rewrites          int64
	LastRewriteOK     bool
}

// ErrRewriteInProgress is returned by StartRewrite when one is already running.
var ErrRewriteInProgress = errors.New("Background append only file rewriting already in progress")

// Persister is the part of the AOF that commands need.
type Persister interface {
	Info() PersistInfo
	// StartRewrite begins a background rewrite and returns immediately.
	StartRewrite() error
}

func registerPersist(t *Table) {
	t.add(Spec{Name: "bgrewriteaof", Arity: 1, Flags: []string{FlagAdmin}, Handler: cmdBgRewriteAOF})
}

func cmdBgRewriteAOF(c *Ctx) {
	if c.Env.AOF == nil {
		c.W.WriteError("ERR AOF is not enabled")
		return
	}
	if err := c.Env.AOF.StartRewrite(); err != nil {
		c.W.WriteError("ERR " + err.Error())
		return
	}
	c.W.WriteSimple("Background append only file rewriting started")
}

func (e *Env) appendOnly() string {
	if e.AOF == nil {
		return "no"
	}
	return "yes"
}

func writePersistenceInfo(sb *strings.Builder, e *Env) {
	if e.AOF == nil {
		sb.WriteString("aof_enabled:0\r\n")
		return
	}
	i := e.AOF.Info()
	b2i := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	fmt.Fprintf(sb, "aof_enabled:1\r\naof_path:%s\r\naof_fsync_policy:%s\r\n", i.Path, i.Policy)
	fmt.Fprintf(sb, "aof_current_size:%d\r\naof_base_size:%d\r\n", i.Size, i.BaseSize)
	fmt.Fprintf(sb, "aof_rewrite_in_progress:%d\r\naof_rewrites:%d\r\n", b2i(i.RewriteInProgress), i.Rewrites)
}

package command

import (
	"bytes"
	"errors"
	"strings"

	"ember/internal/resp"
)

// Replayer returns a function that executes one logged command against the
// store, discarding the reply. Any error reply is returned as an error, since
// a command that succeeded when it was logged must succeed again on replay.
func (e *Env) Replayer() func(args [][]byte) error {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	c := &Ctx{W: w, Store: e.Store, Client: &Client{}, Env: e}
	return func(args [][]byte) error {
		buf.Reset()
		c.Args = args
		e.Table.Execute(c)
		if err := w.Flush(); err != nil {
			return err
		}
		if b := buf.Bytes(); len(b) > 0 && b[0] == '-' {
			return errors.New(strings.TrimSpace(string(b[1:])))
		}
		return nil
	}
}

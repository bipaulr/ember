package command

import "ember/internal/resp"

// parseInt parses a canonical base-10 int64 argument, with the same strictness
// as Redis string2ll: no '+', no leading zeros, no spaces.
func parseInt(b []byte) (int64, bool) { return resp.ParseInt(b) }

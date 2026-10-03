package resp

import (
	"bytes"
	"testing"
)

// FuzzParse feeds arbitrary bytes to both parsers. The only property checked is
// that neither panics and both terminate; protocol errors are expected.
func FuzzParse(f *testing.F) {
	seeds := []string{
		"+OK\r\n", "-ERR x\r\n", ":12\r\n", "$3\r\nfoo\r\n", "$-1\r\n",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "*-1\r\n", "PING\r\n",
		"SET a \"b c\"\r\n", "*1\r\n*1\r\n+x\r\n", "$99999999999\r\n",
		"*3\r\n$3\r\nSET\r\n", "\x00\xff\r\n", "'", "\"\\",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 64; i++ {
			if _, err := r.ReadValue(); err != nil {
				break
			}
		}
		r = NewReader(bytes.NewReader(data))
		for i := 0; i < 64; i++ {
			if _, err := r.ReadCommand(); err != nil {
				break
			}
		}
	})
}

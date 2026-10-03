// Command summarize turns bench/results/ember-vs-redis.csv into a markdown
// table of medians. Each cell is the median over the repeated runs; the spread
// column shows the min to max range so noise is visible.
package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type key struct {
	pipeline int
	test     string
	server   string
}

type sample struct{ rps, p50, p99 []float64 }

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: summarize results.csv")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	data := map[key]*sample{}
	runs := map[int]bool{}
	for _, r := range rows[1:] {
		// run,server,pipeline,test,rps,avg,min,p50,p95,p99,max
		if len(r) < 11 {
			continue
		}
		run, _ := strconv.Atoi(r[0])
		p, _ := strconv.Atoi(r[2])
		k := key{p, strings.ToUpper(r[3]), r[1]}
		s := data[k]
		if s == nil {
			s = &sample{}
			data[k] = s
		}
		s.rps = append(s.rps, num(r[4]))
		s.p50 = append(s.p50, num(r[7]))
		s.p99 = append(s.p99, num(r[9]))
		runs[run] = true
	}

	fmt.Printf("Median of %d runs. rps is requests per second; latencies are milliseconds.\n\n", len(runs))
	fmt.Println("| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, p := range []int{1, 16} {
		for _, t := range []string{"SET", "GET", "INCR", "LPUSH"} {
			r, e := data[key{p, t, "redis"}], data[key{p, t, "ember"}]
			if r == nil || e == nil {
				continue
			}
			rr, er := median(r.rps), median(e.rps)
			fmt.Printf("| %s | %d | %.0f | %.0f | %.2fx | %.3f | %.3f | %.3f | %.3f | %s; %s |\n",
				t, p, rr, er, er/rr, median(r.p50), median(e.p50), median(r.p99), median(e.p99),
				spread(r.rps), spread(e.rps))
		}
	}
}

func num(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }

func median(v []float64) float64 {
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	n := len(c)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

func spread(v []float64) string {
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	return fmt.Sprintf("%.0f to %.0f", c[0], c[len(c)-1])
}

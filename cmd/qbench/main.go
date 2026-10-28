// qbench issues the same QUERY_JOB_ADS a condor_q makes and THROWS THE RESULTS AWAY, to
// find out how much of a large condor_q is the schedd delivering ads and how much is
// condor_q itself turning them into output.
//
// Two read modes, because "discard" can mean two different things and the gap between them
// is the interesting number:
//
//	raw   - read each response message's bytes and drop them. No ClassAd is built, nothing
//	        is evaluated. This is the floor: what the schedd can push down the wire.
//	parse - build a ClassAd per ad, as condor_q and every real client must. The delta
//	        against raw is the client-side parse cost that no server change can remove.
//
// Neither mode formats or prints ads, so whatever condor_q costs beyond `parse` is its
// own output path.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"
	"os/exec"
)

func queryAd(constraint, projection string, limit int) *classad.ClassAd {
	ad := classad.New()
	expr, err := classad.ParseExpr(constraint)
	if err != nil {
		expr, _ = classad.ParseExpr("true")
	}
	ad.InsertExpr("Requirements", expr)
	if projection != "" {
		_ = ad.Set("Projection", projection)
	}
	if limit > 0 {
		_ = ad.Set("LimitResults", int64(limit))
	}
	return ad
}

// one issues a single query and returns how many ads came back, discarding every one.
func one(ctx context.Context, addr, constraint, projection string, limit int, parse bool) (int, error) {
	secConfig, err := htcondor.GetSecurityConfigOrDefault(ctx, nil, commands.QUERY_JOB_ADS, "CLIENT", addr)
	if err != nil {
		return 0, fmt.Errorf("security config: %w", err)
	}
	c, err := client.ConnectAndAuthenticate(ctx, addr, secConfig)
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer c.Close()
	stream := c.GetStream()

	req := message.NewMessageForStream(stream)
	if err := req.PutClassAd(ctx, queryAd(constraint, projection, limit)); err != nil {
		return 0, fmt.Errorf("put query: %w", err)
	}
	if err := req.FinishMessage(ctx); err != nil {
		return 0, fmt.Errorf("send query: %w", err)
	}

	n := 0
	for {
		resp := message.NewMessageFromStream(stream)
		if parse {
			ad, err := resp.GetClassAd(ctx)
			if err != nil {
				return n, fmt.Errorf("read ad: %w", err)
			}
			// The terminator carries Owner = 0; everything else is a job ad.
			if owner, ok := ad.EvaluateAttrInt("Owner"); ok && owner == 0 {
				if code, ok := ad.EvaluateAttrInt("ErrorCode"); ok && code != 0 {
					msg, _ := ad.EvaluateAttrString("ErrorString")
					return n, fmt.Errorf("schedd error %d: %s", code, msg)
				}
				return n, nil
			}
			n++
			_ = ad
			continue
		}
		text, err := resp.GetClassAdRaw(ctx)
		if err != nil {
			return n, fmt.Errorf("read raw ad: %w", err)
		}
		// Detect the terminator without parsing: it is the only ad carrying "Owner = 0",
		// and a job ad's Owner is a quoted string, so the test cannot collide with one.
		if strings.Contains(text, "Owner = 0") {
			return n, nil
		}
		n++
	}
}

// probe measures how long the schedd takes to answer the cheapest thing it can, from INSIDE
// this process.
//
// condor_ping would do the same job, but timing it means timing a fork, an exec, a
// configuration parse and a DNS lookup as well -- and under load those dominate. A control
// made that concrete: sixteen CPU burners that never touch the schedd pushed a forked
// condor_ping from 69ms to 3410ms, so every "the event loop stalled" reading taken that way
// was really a reading of host saturation. Connecting in-process costs none of that, and
// cedar caches the security session so repeats are a connect and a command.
//
// onStall runs once per burst of trouble when a probe exceeds stallAfter: the stall is the
// only moment the evidence exists, and by the time a slow sample is reported the daemon is
// idle again. It is rate-limited to one dump per cooldown so a sustained stall does not
// spawn a debugger storm on an already-loaded host.
type probe struct {
	addr       string
	stallAfter time.Duration
	onStall    string
	cooldown   time.Duration

	mu       sync.Mutex
	lat      []float64
	stalls   int
	dumps    int
	lastDump time.Time
}

func (p *probe) once(ctx context.Context) {
	start := time.Now()
	secConfig, err := htcondor.GetSecurityConfigOrDefault(ctx, nil, commands.DC_NOP_READ, "CLIENT", p.addr)
	if err != nil {
		return
	}
	c, err := client.ConnectAndAuthenticate(ctx, p.addr, secConfig)
	if err == nil {
		_ = c.Close()
	}
	d := time.Since(start)

	p.mu.Lock()
	p.lat = append(p.lat, float64(d.Microseconds())/1000)
	slow := p.stallAfter > 0 && d >= p.stallAfter
	if slow {
		p.stalls++
	}
	fire := slow && p.onStall != "" && time.Since(p.lastDump) > p.cooldown
	if fire {
		p.lastDump = time.Now()
		p.dumps++
	}
	p.mu.Unlock()

	if fire {
		// Synchronously, and deliberately: the point is to catch the daemon mid-stall, and a
		// dump started after the probe loop has moved on photographs the wrong moment.
		cmd := exec.CommandContext(ctx, "sh", "-c", p.onStall)
		cmd.Env = append(os.Environ(), fmt.Sprintf("QBENCH_STALL_MS=%d", d.Milliseconds()))
		out, err := cmd.CombinedOutput()
		fmt.Fprintf(os.Stderr, "  !! probe stalled %.0f ms; ran stall dump (err=%v)\n", float64(d.Milliseconds()), err)
		if len(out) > 0 {
			fmt.Fprintf(os.Stderr, "     %s\n", strings.TrimSpace(string(out)))
		}
	}
}

func (p *probe) run(ctx context.Context, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		p.once(ctx)
		select {
		case <-stop:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (p *probe) report() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.lat) == 0 {
		return
	}
	s := append([]float64(nil), p.lat...)
	sort.Float64s(s)
	q := func(f float64) float64 { return s[min(len(s)-1, int(float64(len(s))*f))] }
	fmt.Printf("  probe (in-process DC_NOP_READ): median %.0f ms  p95 %.0f ms  max %.0f ms  "+
		"(%d samples, %d over threshold, %d dumps)\n",
		q(0.5), q(0.95), s[len(s)-1], len(s), p.stalls, p.dumps)
}

func main() {
	addr := flag.String("addr", "", "schedd sinful address")
	constraint := flag.String("const", "true", "constraint expression")
	projection := flag.String("attrs", "", "comma-separated projection (empty = whole ad)")
	limit := flag.Int("limit", 0, "LimitResults (0 = unlimited)")
	mode := flag.String("mode", "raw", "raw (discard bytes) or parse (build ClassAds)")
	reps := flag.Int("reps", 5, "timed repetitions")
	conc := flag.Int("k", 1, "concurrent queries per repetition")
	probeOn := flag.Bool("probe", false, "while the load runs, time DC_NOP_READ in-process (no fork per sample)")
	stallAfter := flag.Duration("probe-stall", time.Second, "a probe at or over this is a stall")
	onStall := flag.String("probe-stall-cmd", "", "shell command to run on a stall, e.g. 'sample $(pgrep -n condor_schedd) 3 -file /tmp/stall.txt'")
	cooldown := flag.Duration("probe-stall-cooldown", 10*time.Second, "minimum gap between stall dumps")
	flag.Parse()
	if *addr == "" {
		fmt.Fprintln(os.Stderr, "-addr is required")
		os.Exit(2)
	}
	parse := *mode == "parse"
	ctx := context.Background()

	// Warm: the first connection pays authentication and session setup, which is not what
	// this measures.
	if _, err := one(ctx, *addr, *constraint, *projection, *limit, parse); err != nil {
		fmt.Fprintf(os.Stderr, "warm-up failed: %v\n", err)
		os.Exit(1)
	}

	var pr *probe
	var stopProbe chan struct{}
	if *probeOn {
		pr = &probe{addr: *addr, stallAfter: *stallAfter, onStall: *onStall, cooldown: *cooldown}
		stopProbe = make(chan struct{})
		go pr.run(ctx, stopProbe)
	}

	var ms []float64
	ads := 0
	for r := 0; r < *reps; r++ {
		var wg sync.WaitGroup
		var mu sync.Mutex
		errs := 0
		start := time.Now()
		for i := 0; i < *conc; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n, err := one(ctx, *addr, *constraint, *projection, *limit, parse)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs++
					fmt.Fprintf(os.Stderr, "  query failed: %v\n", err)
					return
				}
				ads = n
			}()
		}
		wg.Wait()
		if errs > 0 {
			fmt.Fprintf(os.Stderr, "  %d/%d queries failed in rep %d\n", errs, *conc, r)
		}
		ms = append(ms, float64(time.Since(start).Microseconds())/1000)
	}
	if pr != nil {
		close(stopProbe)
	}
	sort.Float64s(ms)
	fmt.Printf("mode=%-5s k=%d ads=%d  median %.0fms  all %v\n",
		*mode, *conc, ads, ms[len(ms)/2], func() []int {
			out := []int{}
			for _, v := range ms {
				out = append(out, int(v))
			}
			return out
		}())
	if pr != nil {
		pr.report()
	}
}

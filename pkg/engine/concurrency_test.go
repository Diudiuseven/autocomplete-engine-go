package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReadWrite hammers the engine with readers and writers.
// Readers must always observe a complete, self-consistent snapshot.
func TestConcurrentReadWrite(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()

	// seed
	for i := 0; i < 50; i++ {
		if err := eng.Upsert(ctx, "t1", fmt.Sprintf("term%03d", i), 1, 1, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// writers: repeated updates to the same terms (accumulation must not be lost)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				select {
				case <-stop:
					return
				default:
				}
				term := fmt.Sprintf("term%03d", (w*50+i)%50)
				if err := eng.Upsert(ctx, "t1", term, 1, 1, time.Time{}); err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
			}
		}(w)
	}

	// readers: searches and lookups
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				select {
				case <-stop:
					return
				default:
				}
				res, err := eng.Search(ctx, "t1", "term", SearchOptions{Limit: 10})
				if err != nil {
					t.Errorf("search: %v", err)
					return
				}
				// consistency: a query reads one complete snapshot.
				// Since every write adds heat=1 and uses=1 together, a
				// field-level mix would show as heat != uses inside the
				// same match/candidate.
				for _, m := range res.Matches {
					if m.Heat != float64(m.Uses) {
						t.Errorf("field-level mix in match %q: heat=%v uses=%v", m.Term, m.Heat, m.Uses)
						return
					}
				}
				// every returned match must be a real indexed term
				for _, m := range res.Matches {
					c, err := eng.Lookup("t1", m.Term)
					if err != nil {
						t.Errorf("match %q vanished: %v", m.Term, err)
						return
					}
					if c.Heat != float64(c.Uses) {
						t.Errorf("field-level mix in candidate %q: heat=%v uses=%v", m.Term, c.Heat, c.Uses)
						return
					}
				}
			}
		}()
	}

	// stats readers
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := eng.Stats("t1"); err != nil {
					t.Errorf("stats: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// accumulation check: total uses across all terms must equal
	// seed (50) + writer updates (4*200) exactly
	var totalUses int64
	for i := 0; i < 50; i++ {
		c, err := eng.Lookup("t1", fmt.Sprintf("term%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		totalUses += c.Uses
	}
	if totalUses != 50+4*200 {
		t.Fatalf("lost updates: total uses=%d, want %d", totalUses, 50+4*200)
	}
}

// TestConcurrentUpdateSameTerm: many goroutines update one term; accumulation
// must be exact (no lost updates).
func TestConcurrentUpdateSameTerm(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := eng.Upsert(ctx, "t1", "hot", 1, 1, time.Time{}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	c, err := eng.Lookup("t1", "hot")
	if err != nil {
		t.Fatal(err)
	}
	if c.Heat != n || c.Uses != n {
		t.Fatalf("lost updates: heat=%v uses=%v, want %d", c.Heat, c.Uses, n)
	}
}

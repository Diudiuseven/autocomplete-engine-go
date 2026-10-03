package autocomplete

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentMixedWorkload(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	ctx := context.Background()

	var writers sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("w%d-%d", w, i%10)
				if err := e.Upsert(ctx, "t1", BatchItem{Display: key}); err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < 2; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for b := 0; b < 10; b++ {
				items := make([]BatchItem, 5)
				for i := range items {
					items[i] = BatchItem{Display: fmt.Sprintf("b%d-%d-%d", w, b, i)}
				}
				if _, _, err := e.Import(ctx, "t1", NewMemorySource(items)); err != nil {
					t.Errorf("import: %v", err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < 2; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("del%d-%d", w, i)
				if err := e.Upsert(ctx, "t1", BatchItem{Display: key}); err != nil {
					t.Errorf("seed: %v", err)
					return
				}
				if err := e.Delete(ctx, "t1", key); err != nil {
					t.Errorf("delete: %v", err)
					return
				}
			}
		}(w)
	}

	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, stats, err := e.Query(ctx, "t1", "w", QueryOptions{Timeout: 50_000_000})
				if err != nil {
					if errors.Is(err, ErrQueryTimeout) {
						continue
					}
					t.Errorf("query: %v", err)
					return
				}
				for i := 1; i < len(got); i++ {
					if got[i-1].Score < got[i].Score {
						t.Errorf("unordered result: %v then %v", got[i-1].Score, got[i].Score)
						return
					}
				}
				if stats.Version == 0 {
					t.Errorf("zero version read")
					return
				}
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()

	ts := e.idx.current().tenant("t1")
	// 40 writer keys + 2*10*5 = 100 batch keys; delete keys all removed.
	if len(ts.entries) != 140 {
		t.Fatalf("unexpected final size: %d", len(ts.entries))
	}
	for k := range ts.entries {
		if len(k) >= 3 && k[:3] == "del" {
			t.Fatalf("deleted key survived: %s", k)
		}
	}
}

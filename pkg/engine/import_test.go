package engine

import (
	"context"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/model"
	"strings"
	"testing"
	"time"
)

func TestImportDedupAccumulates(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	src := NewFileSource(strings.NewReader(`
# comment
coffee,2,1
coffee,3,4
tea,1,1
`))
	res, err := eng.Import(ctx, "t1", src)
	if err != nil {
		t.Fatal(err)
	}
	if res.Entries != 3 || res.Upserts != 2 {
		t.Fatalf("got %+v, want 3 entries / 2 upserts", res)
	}
	c, err := eng.Lookup("t1", "coffee")
	if err != nil {
		t.Fatal(err)
	}
	if c.Heat != 5 || c.Uses != 5 {
		t.Fatalf("dedup accumulation: heat=%v uses=%v, want 5/5", c.Heat, c.Uses)
	}
}

func TestImportAccumulatesIntoExisting(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	if err := eng.Upsert(ctx, "t1", "coffee", 10, 10, time.Time{}); err != nil {
		t.Fatal(err)
	}
	src := NewFileSource(strings.NewReader("coffee,5,5\n"))
	if _, err := eng.Import(ctx, "t1", src); err != nil {
		t.Fatal(err)
	}
	c, _ := eng.Lookup("t1", "coffee")
	if c.Heat != 15 || c.Uses != 15 {
		t.Fatalf("post-import accumulation: heat=%v uses=%v, want 15/15", c.Heat, c.Uses)
	}
}

func TestImportPipeSource(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	pipe, feed := NewPipeSource(4)
	feed <- model.Entry{Term: "latte", Heat: 1, Uses: 1}
	feed <- model.Entry{Term: "latte", Heat: 1, Uses: 1}
	close(feed)
	res, err := eng.Import(ctx, "t1", pipe)
	if err != nil {
		t.Fatal(err)
	}
	if res.Entries != 2 || res.Upserts != 1 {
		t.Fatalf("pipe: %+v", res)
	}
	c, _ := eng.Lookup("t1", "latte")
	if c.Heat != 2 || c.Uses != 2 {
		t.Fatalf("pipe accumulation: %v/%v", c.Heat, c.Uses)
	}
}

func TestImportFailureCommitsNothing(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	if err := eng.Upsert(ctx, "t1", "existing", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// bad line mid-stream: the whole import must be discarded
	src := NewFileSource(strings.NewReader("ok,1,1\nbad\x00,1,1\n"))
	if _, err := eng.Import(ctx, "t1", src); err == nil {
		t.Fatal("expected error for bad entry")
	}
	if _, err := eng.Lookup("t1", "ok"); err != ErrTermNotFound {
		t.Fatalf("failed import polluted state: %v", err)
	}
	if _, err := eng.Lookup("t1", "existing"); err != nil {
		t.Fatalf("existing term lost after failed import: %v", err)
	}
}

func TestImportTabSeparated(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	src := NewFileSource(strings.NewReader("mocha\t2\t2\n"))
	if _, err := eng.Import(ctx, "t1", src); err != nil {
		t.Fatal(err)
	}
	c, _ := eng.Lookup("t1", "mocha")
	if c.Heat != 2 || c.Uses != 2 {
		t.Fatalf("tab parse: %v/%v", c.Heat, c.Uses)
	}
}

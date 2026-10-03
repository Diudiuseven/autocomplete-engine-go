package autocomplete_test

import (
	"context"
	"fmt"

	"github.com/Diudiuseven/autocomplete-engine-go/autocomplete"
)

func Example() {
	cfg := autocomplete.DefaultConfig()
	cfg.EnablePersistence = false

	eng, err := autocomplete.NewEngine(cfg)
	if err != nil {
		panic(err)
	}
	defer eng.Close()

	ctx := context.Background()
	if err := eng.RegisterTenant(ctx, "demo", 0); err != nil {
		panic(err)
	}
	if err := eng.Upsert(ctx, "demo", autocomplete.BatchItem{Display: "Espresso"}); err != nil {
		panic(err)
	}
	if err := eng.Upsert(ctx, "demo", autocomplete.BatchItem{Display: "ESPRESSO"}); err != nil {
		panic(err)
	}
	cands, _, err := eng.Query(ctx, "demo", "espr", autocomplete.QueryOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println(len(cands), cands[0].Entry.Display, cands[0].Match == autocomplete.MatchPrefix)
	// Output: 1 ESPRESSO true
}

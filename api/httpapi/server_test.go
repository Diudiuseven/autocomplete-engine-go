package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/engine"
)

func newTestServer(t *testing.T) (*engine.Engine, *httptest.Server) {
	t.Helper()
	cfg := engine.DefaultConfig()
	cfg.DataDir = ""
	cfg.QueryTimeout = 2 * time.Second
	eng, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(eng).Handler())
	t.Cleanup(func() { srv.Close(); eng.Close() })
	return eng, srv
}

func doJSON(t *testing.T, method, url string, body interface{}) (int, map[string]interface{}) {
	t.Helper()
	var r *http.Request
	var err error
	if body != nil {
		b, _ := json.Marshal(body)
		r, err = http.NewRequest(method, url, bytes.NewReader(b))
	} else {
		r, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestAPIFlow(t *testing.T) {
	_, srv := newTestServer(t)

	// create tenant
	code, _ := doJSON(t, http.MethodPut, srv.URL+"/v1/tenants/t1", map[string]interface{}{})
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d", code)
	}

	// upsert
	code, _ = doJSON(t, http.MethodPost, srv.URL+"/v1/tenants/t1/candidates",
		map[string]interface{}{"term": "coffee", "heat": 2, "uses": 3})
	if code != http.StatusOK {
		t.Fatalf("upsert: %d", code)
	}

	// search
	resp, err := http.Get(srv.URL + "/v1/tenants/t1/search?q=coff")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res engine.SearchResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 1 || res.Matches[0].Term != "coffee" {
		t.Fatalf("search: %+v", res)
	}

	// stats
	resp, _ = http.Get(srv.URL + "/v1/tenants/t1")
	var st map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&st)
	if int(st["candidates"].(float64)) != 1 {
		t.Fatalf("stats: %+v", st)
	}

}

func TestAPIImportRaw(t *testing.T) {
	_, srv := newTestServer(t)
	doJSON(t, http.MethodPut, srv.URL+"/v1/tenants/t1", map[string]interface{}{})

	resp, err := http.Post(srv.URL+"/v1/tenants/t1/import", "text/plain",
		bytes.NewBufferString("latte,1,1\nlatte,1,1\nmocha,2,2\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res engine.ImportResult
	json.NewDecoder(resp.Body).Decode(&res)
	if res.Entries != 3 || res.Upserts != 2 {
		t.Fatalf("import: %+v", res)
	}
}

func TestAPITenantIsolation(t *testing.T) {
	_, srv := newTestServer(t)
	doJSON(t, http.MethodPut, srv.URL+"/v1/tenants/t1", map[string]interface{}{})
	doJSON(t, http.MethodPut, srv.URL+"/v1/tenants/t2", map[string]interface{}{})
	doJSON(t, http.MethodPost, srv.URL+"/v1/tenants/t1/candidates",
		map[string]interface{}{"term": "secret", "heat": 1, "uses": 1})

	resp, _ := http.Get(srv.URL + "/v1/tenants/t2/search?q=secret")
	var res engine.SearchResult
	json.NewDecoder(resp.Body).Decode(&res)
	if len(res.Matches) != 0 {
		t.Fatalf("tenant leak: %+v", res)
	}

	// unknown tenant -> 404
	resp, _ = http.Get(srv.URL + "/v1/tenants/nope/search?q=x")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown tenant status: %d", resp.StatusCode)
	}
}

func TestAPIValidationErrors(t *testing.T) {
	_, srv := newTestServer(t)
	doJSON(t, http.MethodPut, srv.URL+"/v1/tenants/t1", map[string]interface{}{})

	// empty query -> 400
	resp, _ := http.Get(srv.URL + "/v1/tenants/t1/search?q=")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty query status: %d", resp.StatusCode)
	}
	// empty term -> 400
	code, _ := doJSON(t, http.MethodPost, srv.URL+"/v1/tenants/t1/candidates",
		map[string]interface{}{"term": "", "heat": 1, "uses": 1})
	if code != http.StatusBadRequest {
		t.Fatalf("empty term status: %d", code)
	}
}

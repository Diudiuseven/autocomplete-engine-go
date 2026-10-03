// Package httpapi exposes the engine over a JSON HTTP API.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/engine"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/model"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/tenant"
)

// Server wires HTTP routes to an engine.
type Server struct {
	eng *engine.Engine
	mux *http.ServeMux
}

// New builds the API server.
func New(eng *engine.Engine) *Server {
	s := &Server{eng: eng, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	// /v1/tenants/{id}
	s.mux.HandleFunc("/v1/tenants/", s.handleTenants)
}

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	// path: /v1/tenants/{id}[/...]
	rest := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "missing tenant id")
		return
	}
	tenantID := parts[0]
	sub := parts[1:]

	switch {
	case len(sub) == 0 && r.Method == http.MethodPut:
		s.createTenant(w, r, tenantID)
	case len(sub) == 0 && r.Method == http.MethodGet:
		s.getStats(w, r, tenantID)
	case len(sub) == 1 && sub[0] == "search" && r.Method == http.MethodGet:
		s.search(w, r, tenantID)
	case len(sub) == 1 && sub[0] == "import" && r.Method == http.MethodPost:
		s.doImport(w, r, tenantID)
	case len(sub) == 1 && sub[0] == "candidates" && r.Method == http.MethodPost:
		s.upsert(w, r, tenantID)
	case len(sub) == 2 && sub[0] == "candidates" && r.Method == http.MethodDelete:
		s.remove(w, r, tenantID, sub[1])
	default:
		writeError(w, http.StatusNotFound, "unknown route")
	}
}

type createTenantReq struct {
	MaxCandidates int64 `json:"max_candidates"`
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request, id string) {
	var req createTenantReq
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if err := s.eng.CreateTenant(id, tenant.Quota{MaxCandidates: req.MaxCandidates}); err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"tenant": id})
}

type upsertReq struct {
	Term string  `json:"term"`
	Heat float64 `json:"heat"`
	Uses int64   `json:"uses"`
}

func (s *Server) upsert(w http.ResponseWriter, r *http.Request, id string) {
	var req upsertReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := s.eng.Upsert(r.Context(), id, req.Term, req.Heat, req.Uses, time.Time{}); err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) remove(w http.ResponseWriter, r *http.Request, id, term string) {
	term, err := urlPathUnescape(term)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad term encoding")
		return
	}
	if err := s.eng.Delete(r.Context(), id, term); err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) search(w http.ResponseWriter, r *http.Request, id string) {
	q := r.URL.Query().Get("q")
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = n
	}
	res, err := s.eng.Search(r.Context(), id, q, engine.SearchOptions{Limit: limit})
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	if res.Matches == nil {
		res.Matches = []model.Match{}
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) doImport(w http.ResponseWriter, r *http.Request, id string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}
	res, err := s.eng.Import(r.Context(), id, engine.NewFileSource(strings.NewReader(string(body))))
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request, id string) {
	st, err := s.eng.Stats(id)
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeEngineErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrTenantNotFound), errors.Is(err, engine.ErrTermNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, engine.ErrTenantExists):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, engine.ErrQuotaExceeded):
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, engine.ErrQueryTimeout), errors.Is(err, engine.ErrScanLimit):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, engine.ErrEmptyQuery), errors.Is(err, engine.ErrQueryTooLong),
		errors.Is(err, engine.ErrInvalidQuery), errors.Is(err, engine.ErrEmptyTerm),
		errors.Is(err, engine.ErrTermTooLong), errors.Is(err, engine.ErrInvalidTerm),
		errors.Is(err, engine.ErrEmptyTenant), errors.Is(err, engine.ErrNegativeDelta):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

/**
 * @file routes_test
 * @description Route surface tests: probes answer and unknown paths 404.
 */
package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRootHandlerProbes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(newRootHandler(nil, nil, nil, nil))
	t.Cleanup(srv.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}
}

func TestRootHandlerUnknownPath(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(newRootHandler(nil, nil, nil, nil))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// TestRootHandlerRoutesAdminWrites pins that the mux hands every method
// to the admin handler: the top-up is a PUT and must survive routing,
// while wrong methods still meet the handler's own 405 guard.
func TestRootHandlerRoutesAdminWrites(t *testing.T) {
	t.Parallel()
	topped := false
	setter := func(_ *http.Request, _ string, _ int64) error {
		topped = true
		return nil
	}
	srv := httptest.NewServer(newRootHandler(nil, nil, NewAdmin("", nil, setter, nil), nil))
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPut, srv.URL+"/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":5}`))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !topped {
		t.Fatalf("PUT status = %d topped = %v, want the top-up to reach the ledger", resp.StatusCode, topped)
	}

	req, err = http.NewRequest(http.MethodDelete, srv.URL+"/admin/tenants/t1/quota", nil)
	if err != nil {
		t.Fatalf("build DELETE: %v", err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE status = %d, want 405 from the admin method guard", resp.StatusCode)
	}
}

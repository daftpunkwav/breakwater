/**
 * @file models_test
 * @description The model discovery endpoint: the OpenAI list form with
 * sorted client-facing names, method guarding, and its absence when
 * no models are configured.
 */
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// modelsServer serves the discovery endpoint over the given
// client-facing names (nil configures no models) on a live server.
func modelsServer(t *testing.T, models []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(newRootHandler(nil, nil, nil, "test", nil, models))
	t.Cleanup(srv.Close)
	return srv
}

// getModels GETs the discovery route and returns the response; the
// body is closed with the test.
func getModels(t *testing.T, srv *httptest.Server) *http.Response {
	t.Helper()
	resp, err := http.Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestModelsEndpointListsSortedNames(t *testing.T) {
	t.Parallel()
	resp := getModels(t, modelsServer(t, []string{"zeta", "alpha", "mid"}))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Object string `json:"object"`
		Data   []struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
			Created int64  `json:"created"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Object != "list" {
		t.Fatalf("object = %q, want list", body.Object)
	}
	if len(body.Data) != 3 {
		t.Fatalf("data = %v entries, want 3", body.Data)
	}
	for i, want := range []string{"alpha", "mid", "zeta"} {
		got := body.Data[i]
		if got.ID != want {
			t.Errorf("data[%d].id = %q, want %q", i, got.ID, want)
		}
		if got.Object != "model" {
			t.Errorf("data[%d].object = %q, want model", i, got.Object)
		}
		if got.OwnedBy != "breakwater" {
			t.Errorf("data[%d].owned_by = %q, want breakwater", i, got.OwnedBy)
		}
		if got.Created == 0 {
			t.Errorf("data[%d].created is zero", i)
		}
	}
}

// TestModelsEndpointAbsentWithoutModels: no configured models means no
// discovery route, not an empty list pretending the gateway routes
// something.
func TestModelsEndpointAbsentWithoutModels(t *testing.T) {
	t.Parallel()
	resp := getModels(t, modelsServer(t, nil))

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for an unregistered route", resp.StatusCode)
	}
}

func TestModelsEndpointRejectsWrites(t *testing.T) {
	t.Parallel()
	srv := modelsServer(t, []string{"m1"})

	resp, err := http.Post(srv.URL+"/v1/models", "application/json", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 for a write to the discovery route", resp.StatusCode)
	}
}

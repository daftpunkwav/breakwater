/**
 * @file models
 * @description The model discovery endpoint: GET /v1/models in the
 * OpenAI list form.
 *
 * Responsibilities:
 * - List the client-facing model names the gateway routes, so OpenAI-
 *   compatible clients can discover what to ask for
 * - Nothing else: the endpoint is unauthenticated and carries no
 *   tenant data — exactly the names, and nothing the governance
 *   stages would gate
 */
package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

// modelsOwnedBy is the owner stamped on every listed model.
const modelsOwnedBy = "breakwater"

// makeModelsHandler serves GET /v1/models. The list is fixed for the
// process lifetime (it mirrors the static configuration), so it is
// rendered once at construction; the handler only serializes.
func makeModelsHandler(names []string) http.Handler {
	created := time.Now().Unix()
	type listedModel struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	list := make([]listedModel, 0, len(sorted))
	for _, name := range sorted {
		list = append(list, listedModel{ID: name, Object: "model", Created: created, OwnedBy: modelsOwnedBy})
	}
	body := map[string]any{"object": "list", "data": list}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
}

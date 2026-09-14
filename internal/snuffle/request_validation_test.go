package snuffle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func TestRoutesRejectInvalidRequests(t *testing.T) {
	streamedRead, err := (&prompb.ReadRequest{
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for _, layout := range []string{"current", "posthog"} {
		t.Run(layout, func(t *testing.T) {
			t.Setenv("CH_SCHEMA_LAYOUT", layout)
			cfg := ConfigFromEnv()
			cfg.CHAddr = "127.0.0.1:1"
			mux := http.NewServeMux()
			newServer(cfg).routes(mux)

			for _, test := range []struct {
				name        string
				method      string
				path        string
				contentType string
				body        []byte
				wantStatus  int
			}{
				{name: "write method", method: http.MethodGet, path: "/api/v1/write", wantStatus: http.StatusMethodNotAllowed},
				{name: "read method", method: http.MethodGet, path: "/api/v1/read", wantStatus: http.StatusMethodNotAllowed},
				{name: "push method", method: http.MethodGet, path: "/loki/api/v1/push", wantStatus: http.StatusMethodNotAllowed},
				{name: "write compression", path: "/api/v1/write", body: []byte("invalid"), wantStatus: http.StatusBadRequest},
				{name: "read compression", path: "/api/v1/read", body: []byte("invalid"), wantStatus: http.StatusBadRequest},
				{name: "write protobuf", path: "/api/v1/write", body: snappy.Encode(nil, []byte{0xff}), wantStatus: http.StatusBadRequest},
				{name: "read protobuf", path: "/api/v1/read", body: snappy.Encode(nil, []byte{0xff}), wantStatus: http.StatusBadRequest},
				{name: "unsupported streamed read", path: "/api/v1/read", body: snappy.Encode(nil, streamedRead), wantStatus: http.StatusBadRequest},
				{name: "push JSON", path: "/loki/api/v1/push", contentType: "application/json", body: []byte("{"), wantStatus: http.StatusBadRequest},
				{name: "push timestamp", path: "/loki/api/v1/push", contentType: "application/json", body: []byte(`{"streams":[{"stream":{"service_name":"api"},"values":[["invalid","message"]]}]}`), wantStatus: http.StatusBadRequest},
				{name: "invalid tenant", path: "/t/invalid/api/v1/write", wantStatus: http.StatusBadRequest},
				{name: "negative tenant", path: "/t/-1/loki/api/v1/push", wantStatus: http.StatusBadRequest},
			} {
				t.Run(test.name, func(t *testing.T) {
					method := test.method
					if method == "" {
						method = http.MethodPost
					}
					req := httptest.NewRequest(method, test.path, bytes.NewReader(test.body))
					req.Header.Set("Content-Type", test.contentType)
					recorder := httptest.NewRecorder()
					mux.ServeHTTP(recorder, req)
					if recorder.Code != test.wantStatus {
						t.Fatalf("status = %d, want %d: %s", recorder.Code, test.wantStatus, recorder.Body.String())
					}
					if test.wantStatus == http.StatusMethodNotAllowed && recorder.Header().Get("Allow") != http.MethodPost {
						t.Fatalf("Allow = %q, want POST", recorder.Header().Get("Allow"))
					}
					if test.wantStatus == http.StatusMethodNotAllowed && test.path != "/loki/api/v1/push" {
						if recorder.Body.String() != "method not allowed\n" {
							t.Fatalf("unexpected method error: %q", recorder.Body.String())
						}
						return
					}
					var response apiResponseDTO[json.RawMessage]
					if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
						t.Fatal(err)
					}
					if response.Status != "error" || response.Error == "" {
						t.Fatalf("expected an API error, got %#v", response)
					}
				})
			}
		})
	}
}

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mcafeelabs/platform/pkg/sandbox"
	"github.com/mcafeelabs/platform/pkg/servicekit"
)

func TestOrderChargesThroughPayments(t *testing.T) {
	payments := httptest.NewServer(sandbox.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"service": "payments", "requestSandbox": sandbox.FromContext(r.Context())})
	})))
	defer payments.Close()

	t.Setenv("SERVICE_NAME", "orders")
	k, err := servicekit.New()
	if err != nil {
		t.Fatal(err)
	}
	a := &app{k: k}
	a.cfg.Payments.URL = payments.URL
	a.cfg.Payments.APIKey = "k"
	a.routes()
	srv := httptest.NewServer(k.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/orders", nil)
	req.Header.Set("baggage", "sandbox=demo")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got["requestSandbox"] != "demo" || got["paymentsKeyLoaded"] != true {
		t.Fatalf("got %v", got)
	}
	if p := got["payment"].(map[string]any); p["requestSandbox"] != "demo" {
		t.Fatalf("payments did not see the sandbox: %v", p)
	}
	if got["dbError"] != "database not connected" {
		t.Fatalf("db %v", got["dbError"])
	}
}

func TestDSNEscapesPassword(t *testing.T) {
	var c Config
	c.Database.Host, c.Database.Port, c.Database.Name = "db", "5432", "orders"
	c.Database.User, c.Database.Password = "orders", "p@ss/word"
	if dsn := c.DSN(); !strings.Contains(dsn, "p%40ss%2Fword@db:5432/orders") {
		t.Fatal(dsn)
	}
}

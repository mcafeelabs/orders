// Command orders is the demo entry service: POST /orders charges through
// payments (which calls identity), stores the order in its own database,
// and publishes orders.created. It also consumes inventory's stock.changed,
// and GET /events lists what this copy handled.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mcafeelabs/platform/pkg/sandbox"
	"github.com/mcafeelabs/platform/pkg/servicekit"
	"github.com/nats-io/nats.go/jetstream"
)

// Config is config-values.schema.yaml.
type Config struct {
	NATS struct {
		URL string `json:"url"`
	} `json:"nats"`
	Database struct {
		Host     string `json:"host"`
		Port     string `json:"port"`
		Name     string `json:"name"`
		User     string `json:"user"`
		Password string `json:"password"`
	} `json:"database"`
	Payments struct {
		URL    string `json:"url"`
		APIKey string `json:"apiKey"`
	} `json:"payments"`
	FeatureFlags struct {
		CheckoutV2 string `json:"checkoutV2"`
	} `json:"featureFlags"`
	Events struct {
		OrdersCreated string `json:"ordersCreated"`
		StockChanged  string `json:"stockChanged"`
		Queue         string `json:"queue"`
	} `json:"events"`
}

// DSN builds the postgres connection string.
func (c Config) DSN() string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.Database.User, c.Database.Password),
		Host:     c.Database.Host + ":" + c.Database.Port,
		Path:     c.Database.Name,
		RawQuery: "sslmode=disable&connect_timeout=5",
	}
	return u.String()
}

// Event is one stock.changed message this copy handled.
type Event struct {
	Subject string          `json:"subject"`
	Sandbox string          `json:"sandbox"`
	Data    json.RawMessage `json:"data"`
	At      time.Time       `json:"at"`
}

type app struct {
	k   *servicekit.Kit
	cfg Config

	mu     sync.Mutex
	db     *sql.DB
	js     jetstream.JetStream
	events []Event
}

func (a *app) record(e Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, e)
	if len(a.events) > 100 {
		a.events = a.events[len(a.events)-100:]
	}
}

func (a *app) routes() {
	k := a.k
	k.Mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var payment map[string]any
		if err := k.Call(ctx, http.MethodPost, a.cfg.Payments.URL+"/charge", nil, &payment); err != nil {
			k.JSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		out := k.Info(ctx)
		out["payment"] = payment
		out["checkoutV2"] = a.cfg.FeatureFlags.CheckoutV2
		out["paymentsKeyLoaded"] = a.cfg.Payments.APIKey != ""

		a.mu.Lock()
		db, js := a.db, a.js
		a.mu.Unlock()
		if db != nil {
			var id int64
			err := db.QueryRowContext(ctx, `INSERT INTO orders (sandbox) VALUES ($1) RETURNING id`, sandbox.FromContext(ctx)).Scan(&id)
			if err != nil {
				out["dbError"] = err.Error()
			} else {
				out["orderId"] = id
			}
		} else {
			out["dbError"] = "database not connected"
		}
		if js != nil {
			ev, _ := json.Marshal(map[string]any{"orderId": out["orderId"]})
			if _, err := sandbox.Publish(ctx, js, a.cfg.Events.OrdersCreated, ev); err != nil {
				out["publishError"] = err.Error()
			}
		}
		k.JSON(w, http.StatusOK, out)
	})
	k.Mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		evs := append([]Event{}, a.events...)
		a.mu.Unlock()
		out := k.Info(r.Context())
		out["events"] = evs
		k.JSON(w, http.StatusOK, out)
	})
}

func (a *app) connectDB(ctx context.Context) {
	for {
		db, err := sql.Open("pgx", a.cfg.DSN())
		if err == nil {
			_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS orders (
				id bigserial PRIMARY KEY,
				sandbox text NOT NULL DEFAULT '',
				created_at timestamptz NOT NULL DEFAULT now())`)
		}
		if err == nil {
			a.mu.Lock()
			a.db = db
			a.mu.Unlock()
			a.k.Log.Info("database ready", "host", a.cfg.Database.Host)
			return
		}
		a.k.Log.Warn("database not ready", "err", err)
		if db != nil {
			_ = db.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (a *app) connectNATS(ctx context.Context) {
	js, overrides, err := a.k.NATS(ctx, a.cfg.NATS.URL)
	if err != nil {
		a.k.Log.Error("nats", "err", err)
		return
	}
	for {
		if _, err := sandbox.EnsureStream(ctx, js, "ORDERS", []string{a.cfg.Events.OrdersCreated}); err != nil {
			a.k.Log.Warn("ensure stream", "err", err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	a.mu.Lock()
	a.js = js
	a.mu.Unlock()
	// inventory owns the INVENTORY stream; consume once it exists.
	cons := sandbox.Consumer{
		Service:   a.k.Name,
		Sandbox:   a.k.Sandbox,
		Stream:    "INVENTORY",
		Subjects:  []string{a.cfg.Events.StockChanged},
		Overrides: overrides,
	}
	for ctx.Err() == nil {
		err := cons.Run(ctx, js, func(ctx context.Context, m jetstream.Msg) error {
			a.record(Event{Subject: m.Subject(), Sandbox: sandbox.FromContext(ctx), Data: json.RawMessage(m.Data()), At: time.Now().UTC()})
			return nil
		})
		if err != nil {
			a.k.Log.Warn("consumer", "err", err)
		}
		time.Sleep(3 * time.Second)
	}
}

func main() {
	k, err := servicekit.New()
	if err != nil {
		log.Fatal(err)
	}
	a := &app{k: k}
	if err := k.Decode(&a.cfg); err != nil {
		log.Fatal(fmt.Errorf("config: %w", err))
	}
	a.routes()
	ctx := context.Background()
	go a.connectDB(ctx)
	go a.connectNATS(ctx)
	if err := k.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

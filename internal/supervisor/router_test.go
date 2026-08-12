package supervisor

import (
	"context"
	"strings"
	"testing"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

func meta(table string) *schema.TableMeta {
	return &schema.TableMeta{Schema: "shop", Table: table}
}

func TestRouterDeliversToTheStreamsWatchingATable(t *testing.T) {
	router := NewRouter()
	orders := make(chan cdc.ChangeEvent, 4)
	items := make(chan cdc.ChangeEvent, 4)
	router.Add("shop.orders", orders)
	router.Add("shop.order_items", items)

	if err := router.Route(context.Background(), cdc.ChangeEvent{Meta: meta("orders")}); err != nil {
		t.Fatalf("route: %v", err)
	}

	if len(orders) != 1 {
		t.Errorf("the watching stream received %d events, want 1", len(orders))
	}
	if len(items) != 0 {
		t.Errorf("an unrelated stream received %d events, want 0", len(items))
	}
}

// Two streams over one table each need their own copy: they have separate mappings,
// batching, and positions.
func TestRouterFansOutToEveryStreamOnATable(t *testing.T) {
	router := NewRouter()
	first := make(chan cdc.ChangeEvent, 4)
	second := make(chan cdc.ChangeEvent, 4)
	router.Add("shop.orders", first)
	router.Add("shop.orders", second)

	if err := router.Route(context.Background(), cdc.ChangeEvent{Meta: meta("orders")}); err != nil {
		t.Fatalf("route: %v", err)
	}

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("each stream should receive a copy, got %d and %d", len(first), len(second))
	}
}

func TestRouterIgnoresUnwatchedTables(t *testing.T) {
	router := NewRouter()
	orders := make(chan cdc.ChangeEvent, 4)
	router.Add("shop.orders", orders)

	if err := router.Route(context.Background(), cdc.ChangeEvent{Meta: meta("audit_log")}); err != nil {
		t.Fatalf("route: %v", err)
	}
	if len(orders) != 0 {
		t.Error("an unwatched table produced an event")
	}
}

func TestRouterMatchingIgnoresCase(t *testing.T) {
	router := NewRouter()
	orders := make(chan cdc.ChangeEvent, 4)
	router.Add("SHOP.ORDERS", orders)

	if err := router.Route(context.Background(), cdc.ChangeEvent{Meta: meta("orders")}); err != nil {
		t.Fatalf("route: %v", err)
	}
	if len(orders) != 1 {
		t.Error("table matching should ignore case")
	}
}

// A full queue must block rather than drop, since dropping an event would lose data
// silently. Cancellation is what releases it.
func TestRouterBlocksOnAFullQueueUntilCancelled(t *testing.T) {
	router := NewRouter()
	full := make(chan cdc.ChangeEvent, 1)
	router.Add("shop.orders", full)

	ctx, cancel := context.WithCancel(context.Background())
	if err := router.Route(ctx, cdc.ChangeEvent{Meta: meta("orders")}); err != nil {
		t.Fatalf("first route: %v", err)
	}

	cancel()
	if err := router.Route(ctx, cdc.ChangeEvent{Meta: meta("orders")}); err == nil {
		t.Fatal("expected routing into a full queue to report the cancellation")
	}
}

func TestRouterClosesEveryChannelOnce(t *testing.T) {
	router := NewRouter()
	first := make(chan cdc.ChangeEvent, 1)
	second := make(chan cdc.ChangeEvent, 1)
	router.Add("shop.orders", first)
	router.Add("shop.order_items", second)

	router.Close()
	// Closing twice must not panic: the reader loop and a failure path can both reach it.
	router.Close()

	if _, open := <-first; open {
		t.Error("first channel was not closed")
	}
	if _, open := <-second; open {
		t.Error("second channel was not closed")
	}
}

func TestRouterReportsItsTables(t *testing.T) {
	router := NewRouter()
	router.Add("shop.orders", make(chan cdc.ChangeEvent, 1))
	router.Add("shop.orders", make(chan cdc.ChangeEvent, 1))
	router.Add("shop.order_items", make(chan cdc.ChangeEvent, 1))

	tables := router.Tables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 distinct tables, got %v", tables)
	}
}

// The shared position must be one no stream has passed, or a stream behind the others
// would never receive what it still needs.
func TestSharedStartPositionPicksThePositionCommonToAll(t *testing.T) {
	const source = "ac8fec9f-9576-11f1-810c-16613dc98230"

	got, err := sharedStartPosition(map[string]string{
		"ahead":  source + ":1-100",
		"behind": source + ":1-40",
		"middle": source + ":1-70",
	})
	if err != nil {
		t.Fatalf("shared position: %v", err)
	}
	if got != source+":1-40" {
		t.Fatalf("got %q, want the furthest-behind position", got)
	}
}

func TestSharedStartPositionWithOneStream(t *testing.T) {
	const position = "ac8fec9f-9576-11f1-810c-16613dc98230:1-10"

	got, err := sharedStartPosition(map[string]string{"only": position})
	if err != nil {
		t.Fatalf("shared position: %v", err)
	}
	if got != position {
		t.Fatalf("got %q", got)
	}
}

// Divergent positions cannot be reconciled by choosing between them, and picking one
// would silently skip changes for the other.
func TestSharedStartPositionRefusesDivergentStreams(t *testing.T) {
	_, err := sharedStartPosition(map[string]string{
		"first":  "aaaaaaaa-0000-0000-0000-000000000001:1-10",
		"second": "bbbbbbbb-0000-0000-0000-000000000002:1-10",
	})
	if err == nil {
		t.Fatal("expected divergent positions to be refused")
	}
	if !strings.Contains(err.Error(), "--stream") {
		t.Errorf("error should say how to proceed, got: %v", err)
	}
}

func TestSharedStartPositionRefusesAnUnreadablePosition(t *testing.T) {
	if _, err := sharedStartPosition(map[string]string{"broken": "not a gtid"}); err == nil {
		t.Fatal("expected an unreadable position to be refused")
	}
}

func TestSharedStartPositionRefusesNoStreams(t *testing.T) {
	if _, err := sharedStartPosition(nil); err == nil {
		t.Fatal("expected no streams to be refused")
	}
}

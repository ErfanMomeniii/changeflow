package tail

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestEnumLabelResolvesOneBasedIndex(t *testing.T) {
	labels := []string{"draft", "paid", "shipped", "cancelled"}

	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"first member", int64(1), "draft"},
		{"middle member", int64(2), "paid"},
		{"last member", int64(4), "cancelled"},
		{"invalid value marker", int64(0), "''"},
		{"unsigned storage", uint64(3), "shipped"},
		{"narrow int storage", int32(2), "paid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := enumLabel(labels, tc.value)
			if !ok {
				t.Fatalf("expected resolution for %v", tc.value)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEnumLabelRefusesUnresolvableValues(t *testing.T) {
	labels := []string{"draft", "paid"}

	for _, tc := range []struct {
		name  string
		value any
	}{
		{"index past the last member", int64(3)},
		{"negative index", int64(-1)},
		{"not an integer", "paid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, ok := enumLabel(labels, tc.value); ok {
				t.Fatalf("expected no resolution, got %q", got)
			}
		})
	}
}

// Without column metadata there are no labels, and guessing would silently write
// wrong values, so the caller must fall through to printing the raw number.
func TestEnumLabelWithoutLabelsDoesNotGuess(t *testing.T) {
	if got, ok := enumLabel(nil, int64(1)); ok {
		t.Fatalf("expected no resolution without labels, got %q", got)
	}
}

func TestSetLabelsExpandsBitmask(t *testing.T) {
	labels := []string{"web", "ios", "android", "pos"}

	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"single low bit", int64(1), "{web}"},
		{"two bits", int64(0b0011), "{web,ios}"},
		{"high bit only", int64(0b1000), "{pos}"},
		{"every bit", int64(0b1111), "{web,ios,android,pos}"},
		{"empty set", int64(0), "{}"},
		{"unsigned storage", uint64(0b0110), "{ios,android}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := setLabels(labels, tc.value)
			if !ok {
				t.Fatalf("expected expansion for %v", tc.value)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A bit with no corresponding member must not panic or invent a label.
func TestSetLabelsIgnoresBitsBeyondDeclaredMembers(t *testing.T) {
	got, ok := setLabels([]string{"web", "ios"}, int64(0b1011))
	if !ok {
		t.Fatal("expected expansion")
	}
	if got != "{web,ios}" {
		t.Fatalf("got %q, want {web,ios}", got)
	}
}

func TestSetLabelsWithoutLabelsDoesNotGuess(t *testing.T) {
	if got, ok := setLabels(nil, int64(1)); ok {
		t.Fatalf("expected no expansion without labels, got %q", got)
	}
}

func TestFormatScalarValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"null", nil, "NULL"},
		// Printed exactly, not as a float: 19.90 must not become 19.899999...
		{"exact decimal", decimal.RequireFromString("19.90"), "19.90"},
		{"decimal with many places", decimal.RequireFromString("0.0000000001"), "0.0000000001"},
		// Above 2^63, where a signed read would report a negative number.
		{"unsigned bigint beyond int64", uint64(18446744073709551000), "18446744073709551000"},
		{"utf8 bytes as text", []byte("café"), "café"},
		{"binary bytes as hex", []byte{0x00, 0xff, 0x10}, "0x00ff10"},
		{"string passthrough", "shipped", "shipped"},
		{"signed int", int64(-42), "-42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatScalar(tc.value); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseFiltersIsCaseInsensitive(t *testing.T) {
	f := parseFilters([]string{"Shop.Orders"})

	if !f["shop.orders"] {
		t.Fatalf("expected filter to be lowercased, got %v", f)
	}
}

// No filters means every table, so the map must stay nil rather than becoming an
// empty map that would reject everything.
func TestParseFiltersEmptyMeansAllTables(t *testing.T) {
	if f := parseFilters(nil); f != nil {
		t.Fatalf("expected nil for no filters, got %v", f)
	}
}

func TestTrackedRespectsFilters(t *testing.T) {
	watched := &tailer{want: parseFilters([]string{"shop.orders"})}
	if !watched.tracked("shop", "orders") {
		t.Error("expected shop.orders to be tracked")
	}
	if watched.tracked("shop", "order_items") {
		t.Error("expected shop.order_items to be skipped")
	}
	if !watched.tracked("SHOP", "ORDERS") {
		t.Error("expected table matching to ignore case")
	}

	all := &tailer{want: nil}
	if !all.tracked("anything", "at_all") {
		t.Error("expected every table to be tracked when no filter is set")
	}
}

func TestCollapseFlattensWhitespace(t *testing.T) {
	got := collapse("ALTER TABLE\n  orders\n\tADD COLUMN x INT")
	want := "ALTER TABLE orders ADD COLUMN x INT"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A latin1 column arrives as a Go string holding non-UTF-8 bytes. Printing it
// verbatim produces mojibake, so it must be shown as hex until something knows
// the column's charset and can convert it.
func TestFormatScalarHexEncodesNonUTF8Strings(t *testing.T) {
	latin1Cafe := string([]byte{0x63, 0x61, 0x66, 0xE9}) // "café" in latin1

	if got := formatScalar(latin1Cafe); got != "0x636166e9" {
		t.Fatalf("got %q, want 0x636166e9", got)
	}
	if got := formatScalar("café"); got != "café" {
		t.Fatalf("valid UTF-8 must pass through, got %q", got)
	}
}

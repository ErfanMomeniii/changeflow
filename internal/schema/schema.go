package schema

import (
	"fmt"
	"strings"
)

// Column is one column of a table.
type Column struct {
	Name              string
	Position          int
	DataType          string
	ColumnType        string
	Unsigned          bool
	Nullable          bool
	Generated         bool
	CharacterSet      string
	Collation         string
	NumericPrecision  int
	NumericScale      int
	DateTimePrecision int
	EnumValues        []string
	SetValues         []string
}

// TableMeta describes one table.
type TableMeta struct {
	Schema     string
	Table      string
	Columns    []Column
	PrimaryKey []string
	byName     map[string]int
}

// Name returns the qualified table name.
func (t *TableMeta) Name() string {
	return t.Schema + "." + t.Table
}

// Column returns a column by name.
func (t *TableMeta) Column(name string) (Column, bool) {
	if t.byName == nil {
		t.index()
	}
	i, ok := t.byName[strings.ToLower(name)]
	if !ok {
		return Column{}, false
	}
	return t.Columns[i], true
}

// ColumnNames returns every column name in ordinal order.
func (t *TableMeta) ColumnNames() []string {
	out := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		out[i] = c.Name
	}
	return out
}

// PrimaryKeyPositions returns the ordinal positions of the primary key columns,
// which is what a binlog row needs in order to be keyed.
func (t *TableMeta) PrimaryKeyPositions() ([]int, error) {
	if len(t.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key, so its rows have no stable identity and cannot be replicated idempotently; set mapping.key to a unique index", t.Name())
	}
	out := make([]int, 0, len(t.PrimaryKey))
	for _, name := range t.PrimaryKey {
		c, ok := t.Column(name)
		if !ok {
			return nil, fmt.Errorf("table %s: key column %q is not present in the table", t.Name(), name)
		}
		out = append(out, c.Position)
	}
	return out, nil
}

// ResolveKey validates an explicitly configured key, or falls back to the primary
// key when none is configured.
func (t *TableMeta) ResolveKey(configured []string) ([]string, error) {
	if len(configured) == 0 {
		if len(t.PrimaryKey) == 0 {
			return nil, fmt.Errorf("table %s has no primary key and mapping.key is not set; without a unique key, replays would create duplicates instead of overwriting", t.Name())
		}
		return t.PrimaryKey, nil
	}
	seen := make(map[string]bool, len(configured))
	for _, name := range configured {
		c, ok := t.Column(name)
		if !ok {
			return nil, fmt.Errorf("table %s: mapping.key names column %q, which does not exist", t.Name(), name)
		}
		if seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("table %s: mapping.key lists %q twice", t.Name(), name)
		}
		seen[strings.ToLower(name)] = true
		if c.Generated {
			return nil, fmt.Errorf("table %s: mapping.key names generated column %q, which binlog rows do not carry", t.Name(), name)
		}
		if c.Nullable {
			return nil, fmt.Errorf("table %s: mapping.key names nullable column %q; a null key cannot identify a row", t.Name(), name)
		}
	}
	return configured, nil
}

// SelectColumns applies a mapping's include or exclude list, returning the
// columns a stream will actually read, in ordinal order.
//
// Key columns are always retained: without them a row cannot be identified, so
// excluding one is a mistake rather than a preference.
func (t *TableMeta) SelectColumns(include, exclude, key []string) ([]Column, error) {
	if len(include) > 0 && len(exclude) > 0 {
		return nil, fmt.Errorf("table %s: include and exclude cannot both be set", t.Name())
	}
	inKey := make(map[string]bool, len(key))
	for _, k := range key {
		inKey[strings.ToLower(k)] = true
	}
	var chosen []Column
	switch {
	case len(include) > 0:
		wanted := make(map[string]bool, len(include))
		for _, name := range include {
			if _, ok := t.Column(name); !ok {
				return nil, fmt.Errorf("table %s: mapping.include names column %q, which does not exist", t.Name(), name)
			}
			wanted[strings.ToLower(name)] = true
		}
		for _, k := range key {
			if !wanted[strings.ToLower(k)] {
				return nil, fmt.Errorf("table %s: key column %q is missing from mapping.include; without it a row cannot be identified in the destination", t.Name(), k)
			}
		}
		for _, c := range t.Columns {
			if wanted[strings.ToLower(c.Name)] {
				chosen = append(chosen, c)
			}
		}
	case len(exclude) > 0:
		unwanted := make(map[string]bool, len(exclude))
		for _, name := range exclude {
			if _, ok := t.Column(name); !ok {
				return nil, fmt.Errorf("table %s: mapping.exclude names column %q, which does not exist", t.Name(), name)
			}
			if inKey[strings.ToLower(name)] {
				return nil, fmt.Errorf("table %s: mapping.exclude names key column %q, which must be written", t.Name(), name)
			}
			unwanted[strings.ToLower(name)] = true
		}
		for _, c := range t.Columns {
			if !unwanted[strings.ToLower(c.Name)] && !c.Generated {
				chosen = append(chosen, c)
			}
		}
	default:
		for _, c := range t.Columns {
			if !c.Generated {
				chosen = append(chosen, c)
			}
		}
	}
	if len(chosen) == 0 {
		return nil, fmt.Errorf("table %s: the mapping selects no columns", t.Name())
	}
	return chosen, nil
}

func (t *TableMeta) index() {
	t.byName = make(map[string]int, len(t.Columns))
	for i, c := range t.Columns {
		t.byName[strings.ToLower(c.Name)] = i
	}
}

package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// Loader reads table definitions from a server.
type Loader interface {
	Load(ctx context.Context, schema, table string) (*TableMeta, error)
}

// DBLoader reads definitions from information_schema.
type DBLoader struct {
	DB *sql.DB
}

// Load returns a table's definition, or an error naming the table when it does
// not exist.
func (l DBLoader) Load(ctx context.Context, schemaName, table string) (*TableMeta, error) {
	meta := &TableMeta{Schema: schemaName, Table: table}

	const columnsQuery = `
		SELECT column_name, ordinal_position, data_type, column_type,
		       is_nullable, COALESCE(character_set_name, ''), COALESCE(collation_name, ''),
		       COALESCE(numeric_precision, 0), COALESCE(numeric_scale, 0),
		       COALESCE(datetime_precision, 0), COALESCE(generation_expression, '')
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := l.DB.QueryContext(ctx, columnsQuery, schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s.%s: %w", schemaName, table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			c            Column
			ordinal      int
			isNullable   string
			generationBy string
		)
		if err := rows.Scan(&c.Name, &ordinal, &c.DataType, &c.ColumnType,
			&isNullable, &c.CharacterSet, &c.Collation,
			&c.NumericPrecision, &c.NumericScale, &c.DateTimePrecision, &generationBy); err != nil {
			return nil, fmt.Errorf("scan column of %s.%s: %w", schemaName, table, err)
		}

		// information_schema counts from one; binlog rows count from zero.
		c.Position = ordinal - 1
		c.Nullable = strings.EqualFold(isNullable, "YES")
		c.Unsigned = strings.Contains(strings.ToLower(c.ColumnType), "unsigned")
		// A non-empty generation expression is the authoritative signal, covering
		// both stored and virtual columns. The "extra" column cannot be used for
		// this: MySQL reports "DEFAULT_GENERATED" there for an ordinary column with
		// a default such as CURRENT_TIMESTAMP, which is not a generated column and
		// must still be replicated.
		c.Generated = generationBy != ""

		switch strings.ToLower(c.DataType) {
		case "enum":
			c.EnumValues = parseMemberList(c.ColumnType, "enum")
		case "set":
			c.SetValues = parseMemberList(c.ColumnType, "set")
		}

		meta.Columns = append(meta.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(meta.Columns) == 0 {
		return nil, fmt.Errorf("table %s.%s does not exist or is not visible to this user", schemaName, table)
	}

	pk, err := l.loadPrimaryKey(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}
	meta.PrimaryKey = pk

	return meta, nil
}

func (l DBLoader) loadPrimaryKey(ctx context.Context, schemaName, table string) ([]string, error) {
	const q = `
		SELECT column_name
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY'
		ORDER BY seq_in_index`

	rows, err := l.DB.QueryContext(ctx, q, schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("read primary key of %s.%s: %w", schemaName, table, err)
	}
	defer rows.Close()

	var pk []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan key column of %s.%s: %w", schemaName, table, err)
		}
		pk = append(pk, name)
	}
	return pk, rows.Err()
}

// parseMemberList extracts the labels from an ENUM or SET declaration such as
// enum('draft','paid'). Labels may contain commas and escaped quotes, so the
// declaration is scanned rather than split.
func parseMemberList(columnType, keyword string) []string {
	lower := strings.ToLower(columnType)
	open := strings.Index(lower, keyword+"(")
	if open < 0 {
		return nil
	}
	body := columnType[open+len(keyword)+1:]
	if end := strings.LastIndex(body, ")"); end >= 0 {
		body = body[:end]
	}

	var (
		out     []string
		current strings.Builder
		inQuote bool
	)
	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case ch == '\'' && inQuote && i+1 < len(body) && body[i+1] == '\'':
			// MySQL escapes a quote inside a label by doubling it.
			current.WriteByte('\'')
			i++
		case ch == '\'':
			if inQuote {
				out = append(out, current.String())
				current.Reset()
			}
			inQuote = !inQuote
		case inQuote:
			current.WriteByte(ch)
		}
	}
	return out
}

// Store caches table definitions and reloads them when DDL invalidates a table.
//
// Caching matters because a definition is needed for every row event, and reading
// information_schema per event would put a query on the source for each change.
type Store struct {
	loader Loader

	mu    sync.RWMutex
	cache map[string]*TableMeta
}

// NewStore returns a store backed by a loader.
func NewStore(loader Loader) *Store {
	return &Store{loader: loader, cache: make(map[string]*TableMeta)}
}

// Table returns a table's definition, loading it on first use.
func (s *Store) Table(ctx context.Context, schemaName, table string) (*TableMeta, error) {
	key := cacheKey(schemaName, table)

	s.mu.RLock()
	meta, ok := s.cache[key]
	s.mu.RUnlock()
	if ok {
		return meta, nil
	}

	meta, err := s.loader.Load(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}
	meta.index()

	s.mu.Lock()
	// Another goroutine may have loaded it meanwhile; keep one instance so callers
	// comparing pointers see a stable value.
	if existing, raced := s.cache[key]; raced {
		meta = existing
	} else {
		s.cache[key] = meta
	}
	s.mu.Unlock()

	return meta, nil
}

// Invalidate drops a cached definition, which is what a DDL statement on the
// table requires.
func (s *Store) Invalidate(schemaName, table string) {
	s.mu.Lock()
	delete(s.cache, cacheKey(schemaName, table))
	s.mu.Unlock()
}

// InvalidateAll drops every cached definition, for a DDL statement whose target
// cannot be determined.
func (s *Store) InvalidateAll() {
	s.mu.Lock()
	s.cache = make(map[string]*TableMeta)
	s.mu.Unlock()
}

// Cached reports how many definitions are held, for tests and metrics.
func (s *Store) Cached() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cache)
}

func cacheKey(schemaName, table string) string {
	return strings.ToLower(schemaName) + "." + strings.ToLower(table)
}

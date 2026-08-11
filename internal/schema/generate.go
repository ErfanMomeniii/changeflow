package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Destination column names that carry replication state rather than row data.
//
// They are not optional: the ClickHouse engine deduplicates on them, so a table
// created without them collapses rows by the wrong rule or not at all.
const (
	VersionColumn   = "_version"
	DeletedColumn   = "_is_deleted"
	minClickHouseCH = "23.2"
)

// Generated is a destination schema produced from a table definition.
type Generated struct {
	// Body is the artefact to review and apply.
	Body string
	// Warnings are things worth a human's attention that do not prevent generation.
	Warnings []string
}

// destinationField pairs a source column with the name the destination sees.
type destinationField struct {
	column Column
	name   string
	mapped Mapped
}

// resolveFields applies a mapping and maps every remaining column's type.
//
// Generation and replication read the same type map, which is the point: a
// hand-written mapping drifts from what changeflow actually sends, and the drift
// shows up as silently wrong values rather than as an error.
func resolveFields(meta *TableMeta, include, exclude, key []string, rename map[string]string) ([]destinationField, []string, error) {
	// Warnings are gathered by each generator, since what deserves attention differs
	// by destination.
	selected, err := meta.SelectColumns(include, exclude, key)
	if err != nil {
		return nil, nil, err
	}

	var (
		fields   []destinationField
		warnings []string
	)
	for _, c := range selected {
		mapped, err := Map(c)
		if err != nil {
			return nil, nil, err
		}

		name := c.Name
		if renamed, ok := rename[c.Name]; ok && renamed != "" {
			name = renamed
		}
		if name == VersionColumn || name == DeletedColumn {
			return nil, nil, fmt.Errorf("column %s maps onto %q, which the destination reserves for replication state", c.Name, name)
		}

		fields = append(fields, destinationField{column: c, name: name, mapped: mapped})
	}

	if len(fields) == 0 {
		return nil, nil, fmt.Errorf("table %s: the mapping selects no columns", meta.Name())
	}
	return fields, warnings, nil
}

// GenerateElasticsearch produces an index mapping.
func GenerateElasticsearch(meta *TableMeta, include, exclude, key []string, rename map[string]string, shards, replicas int) (Generated, error) {
	fields, warnings, err := resolveFields(meta, include, exclude, key, rename)
	if err != nil {
		return Generated{}, err
	}

	// Ordered rather than a Go map, so the output is byte-identical between runs and a
	// regenerated file produces a reviewable diff instead of noise.
	properties := make([]string, 0, len(fields))
	for _, f := range fields {
		encodedName, err := json.Marshal(f.name)
		if err != nil {
			return Generated{}, err
		}
		property := fmt.Sprintf(`      %s: { "type": %q }`, encodedName, f.mapped.Elasticsearch)
		if f.mapped.Elasticsearch == "object" {
			// A JSON column is stored but not indexed: its shape is the application's
			// business, and indexing it invites a mapping explosion.
			property = fmt.Sprintf(`      %s: { "type": "object", "enabled": false }`, encodedName)
		}
		properties = append(properties, property)
	}

	if shards < 1 {
		shards = 1
	}
	if replicas < 0 {
		replicas = 1
	}

	body := fmt.Sprintf(`{
  "settings": {
    "number_of_shards": %d,
    "number_of_replicas": %d
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
%s
    }
  }
}
`, shards, replicas, strings.Join(properties, ",\n"))

	warnings = append(warnings,
		"dynamic mapping is disabled: a column added to the source and included in the mapping needs this file regenerated and applied before it can be written",
		"apply this to a new index and move the read alias to it, so a rebuild is atomic and reversible")

	return Generated{Body: body, Warnings: warnings}, nil
}

// GenerateClickHouse produces a CREATE TABLE statement.
func GenerateClickHouse(meta *TableMeta, include, exclude, key []string, rename map[string]string, table string) (Generated, error) {
	fields, warnings, err := resolveFields(meta, include, exclude, key, rename)
	if err != nil {
		return Generated{}, err
	}
	if table == "" {
		return Generated{}, fmt.Errorf("a destination table name is required")
	}

	columns := make([]string, 0, len(fields)+2)
	width := 0
	for _, f := range fields {
		if len(f.name) > width {
			width = len(f.name)
		}
	}
	for _, f := range fields {
		columns = append(columns, fmt.Sprintf("    %-*s %s", width+2, quoteCH(f.name), f.mapped.ClickHouse))
	}
	// Replication state. The engine compares _version to decide which copy of a row
	// wins, and reads _is_deleted to drop rows removed at the source.
	columns = append(columns,
		fmt.Sprintf("    %-*s UInt64", width+2, quoteCH(VersionColumn)),
		fmt.Sprintf("    %-*s UInt8", width+2, quoteCH(DeletedColumn)))

	// The key becomes the sort key, since that is what the engine deduplicates on.
	// Destination names, because that is what the documents carry.
	orderBy := make([]string, 0, len(key))
	for _, k := range key {
		name := k
		if renamed, ok := rename[k]; ok && renamed != "" {
			name = renamed
		}
		orderBy = append(orderBy, quoteCH(name))
	}
	if len(orderBy) == 0 {
		return Generated{}, fmt.Errorf("table %s: a key is required, since it becomes the sort key the engine deduplicates on", meta.Name())
	}

	body := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
(
%s
)
ENGINE = ReplacingMergeTree(%s, %s)
ORDER BY (%s);
`, qualifyCH(table), strings.Join(columns, ",\n"), VersionColumn, DeletedColumn, strings.Join(orderBy, ", "))

	for _, f := range fields {
		if f.column.Nullable {
			warnings = append(warnings, fmt.Sprintf(
				"%s is Nullable, which costs a null mask and blocks some optimisations; consider a default instead", f.name))
		}
	}
	warnings = append(warnings,
		"readers must query this table with FINAL, or with a view that does, until background merges have collapsed duplicate versions",
		"the is_deleted engine parameter requires ClickHouse "+minClickHouseCH+" or newer")

	return Generated{Body: body, Warnings: warnings}, nil
}

func quoteCH(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
}

// qualifyCH quotes a possibly database-qualified table name.
func qualifyCH(table string) string {
	parts := strings.Split(table, ".")
	for i, p := range parts {
		parts[i] = quoteCH(p)
	}
	return strings.Join(parts, ".")
}

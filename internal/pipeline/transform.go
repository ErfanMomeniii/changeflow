// Package pipeline turns source events into destination writes.
package pipeline

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// Dialect selects how values are encoded for a destination. The difference is
// narrow but real: an exact decimal is a string in Elasticsearch, where there is
// no decimal type to hold it, and a number in ClickHouse, where there is.
type Dialect uint8

const (
	DialectElasticsearch Dialect = iota
	DialectClickHouse
)

// maxKeyBytes is Elasticsearch's limit on a document id. Longer keys are digested
// rather than allowed to fail on every write.
const maxKeyBytes = 512

// Zero-date policies.
const (
	zeroDateNull  = "null"
	zeroDateEpoch = "epoch"
	zeroDateError = "error"
)

// field is one column, prepared for encoding.
type field struct {
	// position is the column's index in a row.
	position int
	// name is what the destination sees, after any rename.
	name string
	kind schema.Kind
	// jsonKey is the encoded object key including its quotes and colon, built once
	// so it is not re-escaped per row.
	jsonKey []byte

	enumLabels []string
	setLabels  []string
	// needsCharsetConversion marks a column whose bytes are not UTF-8.
	needsCharsetConversion bool
	// timePrecision is the column's fractional-second precision.
	timePrecision int
}

// Plan is a compiled mapping: the work of deciding which columns to write, what to
// call them, and how to encode each one is done once, not per row.
type Plan struct {
	meta    *schema.TableMeta
	dialect Dialect
	// sourceZone interprets DATETIME, which carries no zone of its own.
	sourceZone   *time.Location
	zeroDate     string
	fields       []field
	keyPositions []int
	keyNames     []string
	// keyFields are the subset of fields forming the key, used to build a tombstone
	// for a destination that expresses deletion as a row rather than as an operation.
	keyFields []field
}

// Compile prepares a mapping for one table and destination, rejecting anything
// unworkable now rather than on the millionth row.
func Compile(meta *schema.TableMeta, mapping config.Mapping, dialect Dialect, sourceZone *time.Location, zeroDate string) (*Plan, error) {
	if meta == nil {
		return nil, fmt.Errorf("transform: no table definition")
	}
	if sourceZone == nil {
		sourceZone = time.UTC
	}
	switch zeroDate {
	case zeroDateNull, zeroDateEpoch, zeroDateError:
	case "":
		zeroDate = zeroDateNull
	default:
		return nil, fmt.Errorf("transform: unknown zero-date policy %q", zeroDate)
	}

	key, err := meta.ResolveKey(mapping.Key)
	if err != nil {
		return nil, err
	}
	selected, err := meta.SelectColumns(mapping.Include, mapping.Exclude, key)
	if err != nil {
		return nil, err
	}

	p := &Plan{
		meta:       meta,
		dialect:    dialect,
		sourceZone: sourceZone,
		zeroDate:   zeroDate,
		keyNames:   key,
	}

	for _, c := range selected {
		mapped, err := schema.Map(c)
		if err != nil {
			return nil, fmt.Errorf("transform: %w", err)
		}

		needsConversion, err := charsetNeedsConversion(c)
		if err != nil {
			return nil, fmt.Errorf("transform: %w", err)
		}

		name := c.Name
		if renamed, ok := mapping.Rename[c.Name]; ok && renamed != "" {
			name = renamed
		}
		encodedKey, err := json.Marshal(name)
		if err != nil {
			return nil, fmt.Errorf("transform: encode field name %q: %w", name, err)
		}

		p.fields = append(p.fields, field{
			position:               c.Position,
			name:                   name,
			kind:                   mapped.Kind,
			jsonKey:                append(encodedKey, ':'),
			enumLabels:             c.EnumValues,
			setLabels:              c.SetValues,
			needsCharsetConversion: needsConversion,
			timePrecision:          c.DateTimePrecision,
		})
	}

	for _, name := range key {
		c, ok := meta.Column(name)
		if !ok {
			return nil, fmt.Errorf("transform: key column %q is not in table %s", name, meta.Name())
		}
		p.keyPositions = append(p.keyPositions, c.Position)

		for _, f := range p.fields {
			if f.position == c.Position {
				p.keyFields = append(p.keyFields, f)
				break
			}
		}
	}

	if dialect == DialectClickHouse && len(p.keyFields) != len(key) {
		// A tombstone carries the key and nothing else, so every key column has to be
		// among the written fields.
		return nil, fmt.Errorf("transform: table %s: every key column must be written for a ClickHouse destination, since a delete is expressed as a row carrying the key", meta.Name())
	}

	return p, nil
}

// charsetNeedsConversion reports whether a column's bytes need transcoding, and
// refuses charsets changeflow cannot convert rather than emitting invalid UTF-8.
func charsetNeedsConversion(c schema.Column) (bool, error) {
	switch strings.ToLower(c.CharacterSet) {
	case "", "utf8", "utf8mb3", "utf8mb4", "ascii", "binary":
		return false, nil
	case "latin1":
		return true, nil
	default:
		return false, fmt.Errorf("column %s uses character set %s, which changeflow cannot convert to UTF-8; exclude the column or migrate it to utf8mb4",
			c.Name, c.CharacterSet)
	}
}

// Fields returns the destination field names this plan writes, in order.
func (p *Plan) Fields() []string {
	out := make([]string, len(p.fields))
	for i, f := range p.fields {
		out[i] = f.name
	}
	return out
}

// KeyNames returns the source columns forming the key.
func (p *Plan) KeyNames() []string { return p.keyNames }

// Apply turns one event into the writes it implies: one document, except for an update
// that changes the key, where the old document must also be deleted — nothing later will
// mention that key again, so it would remain in the destination forever.
func (p *Plan) Apply(ev *cdc.ChangeEvent) ([]cdc.Doc, error) {
	if ev == nil {
		return nil, fmt.Errorf("transform: nil event")
	}

	values := ev.Values()
	if err := p.checkWidth(values); err != nil {
		return nil, err
	}

	if ev.Operation == cdc.OperationDelete {
		key, err := p.key(values)
		if err != nil {
			return nil, err
		}
		doc := cdc.Doc{Key: key, Version: ev.Seq, Deleted: true}
		// Elasticsearch deletes by identifier, so it needs no body. ClickHouse has no
		// delete: the row is superseded by a tombstone carrying its key, so the key
		// values have to travel with it.
		if p.dialect == DialectClickHouse {
			body, err := p.encodeFields(p.keyFields, values)
			if err != nil {
				return nil, err
			}
			doc.Body = body
		}
		return []cdc.Doc{doc}, nil
	}

	newKey, err := p.key(ev.After)
	if err != nil {
		return nil, err
	}
	body, err := p.encode(ev.After)
	if err != nil {
		return nil, err
	}

	if ev.Operation == cdc.OperationUpdate && ev.Before != nil {
		if err := p.checkWidth(ev.Before); err != nil {
			return nil, err
		}
		oldKey, err := p.key(ev.Before)
		if err != nil {
			return nil, err
		}
		if oldKey != newKey {
			return []cdc.Doc{
				{Key: oldKey, Version: ev.Seq, Deleted: true},
				{Key: newKey, Version: ev.Seq, Body: body},
			}, nil
		}
	}

	return []cdc.Doc{{Key: newKey, Version: ev.Seq, Body: body}}, nil
}

func (p *Plan) checkWidth(row cdc.Row) error {
	if row == nil {
		return fmt.Errorf("transform: event for %s carries no values", p.meta.Name())
	}
	if len(row) < len(p.meta.Columns) {
		return fmt.Errorf("transform: row for %s has %d values but the table has %d columns; the cached table definition is stale",
			p.meta.Name(), len(row), len(p.meta.Columns))
	}
	return nil
}

// key builds the destination identity. Parts are escaped before being joined so
// that no two distinct keys can produce the same string.
func (p *Plan) key(row cdc.Row) (string, error) {
	var b strings.Builder
	for i, pos := range p.keyPositions {
		if pos >= len(row) {
			return "", fmt.Errorf("transform: key column %q is beyond the row", p.keyNames[i])
		}
		v := row[pos]
		if v == nil {
			return "", fmt.Errorf("transform: key column %q is null, so the row cannot be identified", p.keyNames[i])
		}
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(escapeKeyPart(keyString(v)))
	}

	key := b.String()
	if len(key) > maxKeyBytes {
		// A digest keeps the write working and stays stable for the same input.
		sum := sha256.Sum256([]byte(key))
		return "h" + hex.EncodeToString(sum[:]), nil
	}
	return key, nil
}

func keyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case uint64:
		return strconv.FormatUint(x, 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case decimal.Decimal:
		return x.String()
	default:
		return fmt.Sprint(v)
	}
}

// escapeKeyPart percent-encodes everything outside an unreserved set, so the
// separator cannot appear inside a part.
func escapeKeyPart(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}
	return b.String()
}

// encode writes the document body. Each value is encoded exactly once here; the
// sinks concatenate these bytes rather than re-marshalling them.
func (p *Plan) encode(row cdc.Row) ([]byte, error) {
	return p.encodeFields(p.fields, row)
}

func (p *Plan) encodeFields(fields []field, row cdc.Row) ([]byte, error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, '{')

	for i, f := range fields {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, f.jsonKey...)

		var err error
		buf, err = p.encodeValue(buf, f, row[f.position])
		if err != nil {
			return nil, err
		}
	}

	return append(buf, '}'), nil
}

// encodeValue appends one value in its destination form. Each kind is encoded by its own
// appender below; this only chooses between them.
func (p *Plan) encodeValue(buf []byte, f field, v any) ([]byte, error) {
	if v == nil {
		return append(buf, "null"...), nil
	}

	switch f.kind {
	case schema.KindBool:
		return appendBool(buf, f, v)
	case schema.KindInt, schema.KindYear, schema.KindTime, schema.KindBit:
		return appendInt(buf, f, v)
	case schema.KindUint:
		return appendUint(buf, f, v)
	case schema.KindFloat:
		return appendFloat(buf, f, v)
	case schema.KindDecimal:
		return p.appendDecimal(buf, f, v)
	case schema.KindEnum:
		return appendEnum(buf, f, v)
	case schema.KindSet:
		return appendSet(buf, f, v)
	case schema.KindJSON:
		return appendRawJSON(buf, f, v)
	case schema.KindBytes:
		return appendBase64(buf, f, v)
	case schema.KindString:
		return appendString(buf, f, v)
	case schema.KindDate, schema.KindDateTime, schema.KindTimestamp:
		return p.appendTime(buf, f, v)
	default:
		return nil, fmt.Errorf("transform: field %s has unhandled kind %s", f.name, f.kind)
	}
}

func appendBool(buf []byte, f field, v any) ([]byte, error) {
	n, ok := asInt(v)
	if !ok {
		return nil, fmt.Errorf("transform: field %s expected an integer for a boolean, got %T", f.name, v)
	}
	if n != 0 {
		return append(buf, "true"...), nil
	}
	return append(buf, "false"...), nil
}

func appendInt(buf []byte, f field, v any) ([]byte, error) {
	if x, ok := v.(uint64); ok {
		return strconv.AppendUint(buf, x, 10), nil
	}
	n, ok := asInt(v)
	if !ok {
		return nil, fmt.Errorf("transform: field %s expected an integer, got %T", f.name, v)
	}
	return strconv.AppendInt(buf, n, 10), nil
}

func appendUint(buf []byte, f field, v any) ([]byte, error) {
	switch x := v.(type) {
	case uint64:
		// Appended as digits, never via a float, so values above 2^53 stay exact.
		return strconv.AppendUint(buf, x, 10), nil
	case int64:
		if x < 0 {
			return nil, fmt.Errorf("transform: field %s is unsigned but arrived negative (%d), which means the binlog carried no signedness metadata; set binlog_row_metadata=FULL", f.name, x)
		}
		return strconv.AppendInt(buf, x, 10), nil
	default:
		n, ok := asInt(v)
		if !ok {
			return nil, fmt.Errorf("transform: field %s expected an unsigned integer, got %T", f.name, v)
		}
		return strconv.AppendInt(buf, n, 10), nil
	}
}

func appendFloat(buf []byte, f field, v any) ([]byte, error) {
	switch x := v.(type) {
	case float32:
		return strconv.AppendFloat(buf, float64(x), 'g', -1, 32), nil
	case float64:
		return strconv.AppendFloat(buf, x, 'g', -1, 64), nil
	default:
		return nil, fmt.Errorf("transform: field %s expected a float, got %T", f.name, v)
	}
}

func (p *Plan) appendDecimal(buf []byte, f field, v any) ([]byte, error) {
	text, err := decimalText(v)
	if err != nil {
		return nil, fmt.Errorf("transform: field %s: %w", f.name, err)
	}
	// Quoted for Elasticsearch, which has no exact decimal type and would
	// otherwise round the value; bare for ClickHouse, whose column is exact.
	if p.dialect == DialectElasticsearch {
		return appendJSONString(buf, text), nil
	}
	return append(buf, text...), nil
}

func appendEnum(buf []byte, f field, v any) ([]byte, error) {
	n, ok := asInt(v)
	if !ok {
		return nil, fmt.Errorf("transform: field %s expected an enum member number, got %T", f.name, v)
	}
	switch {
	case n == 0:
		// MySQL's marker for a value that failed validation.
		return appendJSONString(buf, ""), nil
	case n >= 1 && int(n) <= len(f.enumLabels):
		return appendJSONString(buf, f.enumLabels[n-1]), nil
	default:
		return nil, fmt.Errorf("transform: field %s has enum member number %d, but only %d members are declared", f.name, n, len(f.enumLabels))
	}
}

// appendSet writes a set column as an array of the labels its bitmask selects.
func appendSet(buf []byte, f field, v any) ([]byte, error) {
	bits, ok := asInt(v)
	if !ok {
		return nil, fmt.Errorf("transform: field %s expected a set bitmask, got %T", f.name, v)
	}

	buf = append(buf, '[')
	first := true
	for i, label := range f.setLabels {
		if bits&(1<<uint(i)) == 0 {
			continue
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = appendJSONString(buf, label)
	}
	return append(buf, ']'), nil
}

// appendRawJSON passes a JSON column through unchanged: re-encoding could reorder keys or
// lose numeric precision that the source preserved.
func appendRawJSON(buf []byte, f field, v any) ([]byte, error) {
	var raw []byte
	switch x := v.(type) {
	case []byte:
		raw = x
	case string:
		raw = []byte(x)
	default:
		return nil, fmt.Errorf("transform: field %s expected a JSON document, got %T", f.name, v)
	}

	if len(raw) == 0 {
		return append(buf, "null"...), nil
	}
	return append(buf, raw...), nil
}

func appendBase64(buf []byte, f field, v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return appendJSONString(buf, base64.StdEncoding.EncodeToString(x)), nil
	case string:
		return appendJSONString(buf, base64.StdEncoding.EncodeToString([]byte(x))), nil
	default:
		return nil, fmt.Errorf("transform: field %s expected bytes, got %T", f.name, v)
	}
}

func appendString(buf []byte, f field, v any) ([]byte, error) {
	text, err := stringValue(f, v)
	if err != nil {
		return nil, err
	}
	return appendJSONString(buf, text), nil
}

// stringValue converts a column's bytes to UTF-8 text.
func stringValue(f field, v any) (string, error) {
	var raw string
	switch x := v.(type) {
	case string:
		raw = x
	case []byte:
		raw = string(x)
	default:
		return "", fmt.Errorf("transform: field %s expected text, got %T", f.name, v)
	}

	if f.needsCharsetConversion {
		// In latin1 every byte is its own code point, so the conversion is a
		// widening rather than a table lookup.
		var b strings.Builder
		b.Grow(len(raw) + 8)
		for i := 0; i < len(raw); i++ {
			b.WriteRune(rune(raw[i]))
		}
		return b.String(), nil
	}

	if !utf8.ValidString(raw) {
		return "", fmt.Errorf("transform: field %s holds bytes that are not valid UTF-8 (%s); its declared character set claims otherwise",
			f.name, hex.EncodeToString([]byte(raw)))
	}
	return raw, nil
}

// appendTime renders a temporal value as RFC 3339 in UTC.
//
// A DATETIME is wall-clock text with no zone, so it is interpreted in the source's
// zone. A TIMESTAMP is already an instant in UTC, and converting it again would
// shift it.
func (p *Plan) appendTime(buf []byte, f field, v any) ([]byte, error) {
	switch x := v.(type) {
	case time.Time:
		return appendJSONString(buf, x.UTC().Format(time.RFC3339Nano)), nil

	case string:
		if isZeroDate(x) {
			switch p.zeroDate {
			case zeroDateNull:
				return append(buf, "null"...), nil
			case zeroDateEpoch:
				return appendJSONString(buf, time.Unix(0, 0).UTC().Format(time.RFC3339)), nil
			default:
				return nil, fmt.Errorf("transform: field %s holds the zero date %q, which no destination can represent; set mapping.on_zero_date to null or epoch to accept it", f.name, x)
			}
		}

		zone := p.sourceZone
		if f.kind == schema.KindTimestamp {
			// Already UTC as stored by MySQL.
			zone = time.UTC
		}
		parsed, err := parseMySQLTime(x, zone)
		if err != nil {
			return nil, fmt.Errorf("transform: field %s: %w", f.name, err)
		}
		return appendJSONString(buf, parsed.UTC().Format(time.RFC3339Nano)), nil

	default:
		return nil, fmt.Errorf("transform: field %s expected a date or time, got %T", f.name, v)
	}
}

func isZeroDate(s string) bool {
	return strings.HasPrefix(s, "0000-00-00")
}

func parseMySQLTime(s string, zone *time.Location) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"15:04:05.999999999",
	} {
		if t, err := time.ParseInLocation(layout, s, zone); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as a date or time", s)
}

func decimalText(v any) (string, error) {
	switch x := v.(type) {
	case decimal.Decimal:
		// Printed at the scale the column declares, so a trailing zero survives:
		// as a keyword term, "19.9" and "19.90" are different values.
		places := -x.Exponent()
		if places < 0 {
			places = 0
		}
		return x.StringFixed(places), nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case float64:
		// A float has already lost exactness, but refusing the write would be worse
		// than recording what arrived.
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("expected an exact decimal, got %T", v)
	}
}

func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	case uint64:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint:
		return int64(x), true
	default:
		return 0, false
	}
}

// appendJSONString writes a quoted, escaped string without allocating an
// intermediate value.
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			buf = append(buf, '\\', '"')
		case c == '\\':
			buf = append(buf, '\\', '\\')
		case c == '\n':
			buf = append(buf, '\\', 'n')
		case c == '\r':
			buf = append(buf, '\\', 'r')
		case c == '\t':
			buf = append(buf, '\\', 't')
		case c < 0x20:
			const hexDigits = "0123456789abcdef"
			buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0x0f])
		default:
			buf = append(buf, c)
		}
	}
	return append(buf, '"')
}

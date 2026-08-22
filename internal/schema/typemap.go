package schema

import (
	"fmt"
	"strings"
)

// Kind is how changeflow represents a column's value internally, independent of
// any particular destination.
type Kind uint8

const (
	KindInvalid Kind = iota
	KindBool
	KindInt
	KindUint
	KindFloat
	// KindDecimal is carried as text so the value stays exact. A float would
	// round it, and money does not tolerate that.
	KindDecimal
	KindString
	KindBytes
	KindJSON
	KindDate
	// KindDateTime is wall-clock text with no zone attached.
	KindDateTime
	// KindTimestamp is an instant, stored by MySQL as UTC.
	KindTimestamp
	KindTime
	KindYear
	KindEnum
	KindSet
	KindBit
)

var kindNames = map[Kind]string{
	KindInvalid: "invalid", KindBool: "bool", KindInt: "int", KindUint: "uint",
	KindFloat: "float", KindDecimal: "decimal", KindString: "string",
	KindBytes: "bytes", KindJSON: "json", KindDate: "date",
	KindDateTime: "datetime", KindTimestamp: "timestamp", KindTime: "time",
	KindYear: "year", KindEnum: "enum", KindSet: "set", KindBit: "bit",
}

func (k Kind) String() string {
	if name, ok := kindNames[k]; ok {
		return name
	}
	return "unknown"
}

// Mapped is one column's representation in each destination.
type Mapped struct {
	Kind          Kind
	Elasticsearch string
	ClickHouse    string
}

// Map converts a MySQL column into its destination types.
//
// Every decision here is one that silently corrupts data when made carelessly:
// an unsigned 64-bit integer wrapping negative, a decimal rounded through a
// float, or a wall-clock DATETIME being treated as an instant.
func Map(c Column) (Mapped, error) {
	if c.Generated {
		return Mapped{}, fmt.Errorf("column %s is generated, so it is absent from binlog row images and would always replicate as null", c.Name)
	}
	kind, es, ch, err := mapType(c)
	if err != nil {
		return Mapped{}, err
	}
	lowCardinality := strings.HasPrefix(ch, "LowCardinality(")
	if lowCardinality {
		ch = strings.TrimSuffix(strings.TrimPrefix(ch, "LowCardinality("), ")")
	}
	if c.Nullable {
		ch = "Nullable(" + ch + ")"
	}
	if lowCardinality {
		ch = "LowCardinality(" + ch + ")"
	}
	return Mapped{Kind: kind, Elasticsearch: es, ClickHouse: ch}, nil
}

// Supported reports whether a column can be replicated at all.
func Supported(c Column) bool {
	_, err := Map(c)
	return err == nil
}

func mapType(c Column) (Kind, string, string, error) {
	switch strings.ToLower(c.DataType) {
	case "tinyint":
		if !c.Unsigned && strings.HasPrefix(strings.ToLower(c.ColumnType), "tinyint(1)") {
			return KindBool, "boolean", "Bool", nil
		}
		return intTypes(c, "byte", "Int8", "short", "UInt8")
	case "smallint":
		return intTypes(c, "short", "Int16", "integer", "UInt16")
	case "mediumint":
		return intTypes(c, "integer", "Int32", "long", "UInt32")
	case "int", "integer":
		return intTypes(c, "integer", "Int32", "long", "UInt32")
	case "bigint":
		return intTypes(c, "long", "Int64", "unsigned_long", "UInt64")
	case "decimal", "numeric":
		precision, scale := c.NumericPrecision, c.NumericScale
		if precision <= 0 {
			precision, scale = 10, 0
		}
		return KindDecimal, "keyword", fmt.Sprintf("Decimal(%d, %d)", precision, scale), nil
	case "float":
		return KindFloat, "float", "Float32", nil
	case "double", "double precision", "real":
		return KindFloat, "double", "Float64", nil
	case "char", "varchar":
		return KindString, "keyword", "String", nil
	case "tinytext", "text", "mediumtext", "longtext":
		return KindString, "text", "String", nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return KindBytes, "binary", "String", nil
	case "json":
		return KindJSON, "object", "String", nil
	case "date":
		return KindDate, "date", "Date32", nil
	case "datetime":
		return KindDateTime, "date", fmt.Sprintf("DateTime64(%d)", c.DateTimePrecision), nil
	case "timestamp":
		return KindTimestamp, "date", fmt.Sprintf("DateTime64(%d, 'UTC')", c.DateTimePrecision), nil
	case "time":
		return KindTime, "long", "Int64", nil
	case "year":
		return KindYear, "short", "UInt16", nil
	case "enum":
		if len(c.EnumValues) == 0 {
			return 0, "", "", fmt.Errorf("column %s is an ENUM but no member labels are known; the binlog carries only member numbers, so writing them would corrupt the value (check binlog_row_metadata=FULL)", c.Name)
		}
		return KindEnum, "keyword", "LowCardinality(String)", nil
	case "set":
		if len(c.SetValues) == 0 {
			return 0, "", "", fmt.Errorf("column %s is a SET but no member labels are known; the binlog carries only a bitmask (check binlog_row_metadata=FULL)", c.Name)
		}
		return KindSet, "keyword", "Array(String)", nil
	case "bit":
		return KindBit, "long", "UInt64", nil
	case "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection":
		return 0, "", "", fmt.Errorf("column %s has spatial type %s, which changeflow does not replicate; exclude it from the mapping", c.Name, c.DataType)
	default:
		return 0, "", "", fmt.Errorf("column %s has unrecognised type %q; changeflow refuses to guess how to represent it", c.Name, c.DataType)
	}
}

func intTypes(c Column, signedES, signedCH, unsignedES, unsignedCH string) (Kind, string, string, error) {
	if c.Unsigned {
		return KindUint, unsignedES, unsignedCH, nil
	}
	return KindInt, signedES, signedCH, nil
}

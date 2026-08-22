package tail

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/shopspring/decimal"
)

// Config controls one tail run.
type Config struct {
	Host       string
	Port       uint16
	User       string
	Password   string
	ServerID   uint32
	Tables     []string
	CaptureDir string
	Duration   time.Duration
	StartGTID  string
	Out        io.Writer
}

// Tail streams binlog events until the context is cancelled or Duration elapses.
func Tail(ctx context.Context, cfg Config) error {
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:                cfg.ServerID,
		Flavor:                  "mysql",
		Host:                    cfg.Host,
		Port:                    cfg.Port,
		User:                    cfg.User,
		Password:                cfg.Password,
		UseDecimal:              true,
		ParseTime:               false,
		TimestampStringLocation: time.UTC,
		HeartbeatPeriod:         5 * time.Second,
		ReadTimeout:             90 * time.Second,
	})
	defer syncer.Close()
	gtidSet, err := mysql.ParseMysqlGTIDSet(cfg.StartGTID)
	if err != nil {
		return fmt.Errorf("parse start GTID %q: %w", cfg.StartGTID, err)
	}
	streamer, err := syncer.StartSyncGTID(gtidSet)
	if err != nil {
		return fmt.Errorf("start sync from %s: %w", cfg.StartGTID, err)
	}
	fmt.Fprintf(cfg.Out, "streaming from %s as server_id=%d\n\n", cfg.StartGTID, cfg.ServerID)
	t := &tailer{cfg: cfg, want: parseFilters(cfg.Tables)}
	if cfg.CaptureDir != "" {
		if err := os.MkdirAll(cfg.CaptureDir, 0o755); err != nil {
			return fmt.Errorf("create capture dir: %w", err)
		}
		defer t.writeManifest()
	}
	for {
		ev, err := streamer.GetEvent(ctx)
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			fmt.Fprintf(cfg.Out, "\nstopped: %d row events seen, %d fixtures captured\n", t.rowEvents, len(t.captured))
			return nil
		case err != nil:
			return fmt.Errorf("read event: %w", err)
		}
		if err := t.handle(ev); err != nil {
			return err
		}
	}
}

type tailer struct {
	cfg       Config
	want      map[string]bool
	gtid      string
	rowEvents int
	captured  []fixture
}

type fixture struct {
	File  string `json:"file"`
	Type  string `json:"type"`
	Table string `json:"table,omitempty"`
}

func parseFilters(tables []string) map[string]bool {
	if len(tables) == 0 {
		return nil
	}
	m := make(map[string]bool, len(tables))
	for _, t := range tables {
		m[strings.ToLower(t)] = true
	}
	return m
}

func (t *tailer) tracked(schema, table string) bool {
	if t.want == nil {
		return true
	}
	return t.want[strings.ToLower(schema+"."+table)]
}

func (t *tailer) handle(ev *replication.BinlogEvent) error {
	switch e := ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		return t.capture(ev, "format_description", "")
	case *replication.GTIDEvent:
		set, err := e.GTIDNext()
		if err == nil {
			t.gtid = set.String()
		}
		return nil
	case *replication.XIDEvent:
		t.gtid = ""
		return nil
	case *replication.TableMapEvent:
		schema, table := string(e.Schema), string(e.Table)
		if !t.tracked(schema, table) {
			return nil
		}
		return t.capture(ev, "table_map", schema+"."+table)
	case *replication.RowsEvent:
		schema, table := string(e.Table.Schema), string(e.Table.Table)
		if !t.tracked(schema, table) {
			return nil
		}
		t.rowEvents++
		if err := t.printRows(ev.Header.EventType, e); err != nil {
			return err
		}
		return t.capture(ev, strings.ToLower(ev.Header.EventType.String()), schema+"."+table)
	case *replication.QueryEvent:
		q := strings.TrimSpace(string(e.Query))
		if q != "" && !strings.EqualFold(q, "BEGIN") {
			fmt.Fprintf(t.cfg.Out, "query  %s\n", collapse(q))
		}
		return nil
	}
	return nil
}

func (t *tailer) printRows(typ replication.EventType, e *replication.RowsEvent) error {
	names := e.Table.ColumnNameString()
	if len(names) == 0 {
		return errors.New("binlog carries no column names: set binlog_row_metadata=FULL on the source (see preflight)")
	}
	tbl := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Table)
	gtid := ""
	if t.gtid != "" {
		gtid = "  gtid=" + t.gtid
	}
	switch typ {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for _, row := range e.Rows {
			fmt.Fprintf(t.cfg.Out, "insert %s %s%s\n", tbl, formatRow(e.Table, names, row), gtid)
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for _, row := range e.Rows {
			fmt.Fprintf(t.cfg.Out, "delete %s %s%s\n", tbl, formatKey(e.Table, names, row), gtid)
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			before, after := e.Rows[i], e.Rows[i+1]
			diff := formatDiff(e.Table, names, before, after)
			if diff == "" {
				diff = "(no visible column change)"
			}
			fmt.Fprintf(t.cfg.Out, "update %s %s %s%s\n", tbl, formatKey(e.Table, names, before), diff, gtid)
		}
	}
	return nil
}

func keyIndexes(meta *replication.TableMapEvent) (idx []int, isKey bool) {
	if len(meta.PrimaryKey) > 0 {
		out := make([]int, 0, len(meta.PrimaryKey))
		for _, i := range meta.PrimaryKey {
			out = append(out, int(i))
		}
		return out, true
	}
	return []int{0}, false
}

func formatKey(meta *replication.TableMapEvent, names []string, row []any) string {
	idx, isKey := keyIndexes(meta)
	var b strings.Builder
	for n, i := range idx {
		if i >= len(row) || i >= len(names) {
			continue
		}
		if n > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%s", names[i], formatValue(meta, i, row[i]))
	}
	if !isKey {
		b.WriteString(" (no primary key)")
	}
	return b.String()
}

func formatRow(meta *replication.TableMapEvent, names []string, row []any) string {
	var b strings.Builder
	for i := range row {
		if i >= len(names) {
			break
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%s", names[i], formatValue(meta, i, row[i]))
	}
	return b.String()
}

func formatDiff(meta *replication.TableMapEvent, names []string, before, after []any) string {
	var parts []string
	for i := range after {
		if i >= len(names) || i >= len(before) {
			break
		}
		was, now := formatValue(meta, i, before[i]), formatValue(meta, i, after[i])
		if was != now {
			parts = append(parts, fmt.Sprintf("%s: %s -> %s", names[i], was, now))
		}
	}
	return strings.Join(parts, ", ")
}

func formatValue(meta *replication.TableMapEvent, col int, v any) string {
	if v == nil {
		return "NULL"
	}
	if meta.IsEnumColumn(col) {
		if s, ok := enumLabel(meta.EnumStrValueMap()[col], v); ok {
			return s
		}
	}
	if meta.IsSetColumn(col) {
		if s, ok := setLabels(meta.SetStrValueMap()[col], v); ok {
			return s
		}
	}
	return formatScalar(v)
}

func formatScalar(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case decimal.Decimal:
		places := -x.Exponent()
		if places < 0 {
			places = 0
		}
		return x.StringFixed(places)
	case []byte:
		if utf8.Valid(x) {
			return string(x)
		}
		return "0x" + hex.EncodeToString(x)
	case string:
		if utf8.ValidString(x) {
			return x
		}
		return "0x" + hex.EncodeToString([]byte(x))
	case time.Time:
		return x.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

func enumLabel(labels []string, v any) (string, bool) {
	if len(labels) == 0 {
		return "", false
	}
	idx, ok := toInt(v)
	if !ok {
		return "", false
	}
	switch {
	case idx == 0:
		return "''", true
	case idx >= 1 && int(idx) <= len(labels):
		return labels[idx-1], true
	default:
		return "", false
	}
}

func setLabels(labels []string, v any) (string, bool) {
	if len(labels) == 0 {
		return "", false
	}
	bits, ok := toInt(v)
	if !ok {
		return "", false
	}
	out := make([]string, 0, len(labels))
	for i, label := range labels {
		if bits&(1<<uint(i)) != 0 {
			out = append(out, label)
		}
	}
	return "{" + strings.Join(out, ",") + "}", true
}

func toInt(v any) (int64, bool) {
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
	default:
		return 0, false
	}
}

func (t *tailer) capture(ev *replication.BinlogEvent, kind, table string) error {
	if t.cfg.CaptureDir == "" {
		return nil
	}
	name := fmt.Sprintf("%04d-%s.bin", len(t.captured), kind)
	if err := os.WriteFile(filepath.Join(t.cfg.CaptureDir, name), ev.RawData, 0o644); err != nil {
		return fmt.Errorf("write fixture %s: %w", name, err)
	}
	t.captured = append(t.captured, fixture{File: name, Type: kind, Table: table})
	return nil
}

func (t *tailer) writeManifest() {
	if len(t.captured) == 0 {
		return
	}
	body, err := json.MarshalIndent(t.captured, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(t.cfg.CaptureDir, "manifest.json"), body, 0o644)
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

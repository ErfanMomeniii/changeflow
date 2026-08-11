// Package dlq records documents a destination refused permanently.
//
// The pipeline advances its position once a rejection is recorded, so recording has
// to be durable before Record returns. If a record were lost, the document would be
// lost with it and nothing would indicate anything was missing.
package dlq

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/sink"
)

// Record is one refused document.
type Record struct {
	RecordedAt time.Time       `json:"recorded_at"`
	Stream     string          `json:"stream"`
	Key        string          `json:"key"`
	Version    uint64          `json:"version"`
	Deleted    bool            `json:"deleted,omitempty"`
	Status     int             `json:"status,omitempty"`
	Reason     string          `json:"reason"`
	BodyBytes  int             `json:"body_bytes"`
	Body       json.RawMessage `json:"body,omitempty"`
}

// Options configures a writer.
type Options struct {
	// Dir holds the files, one per stream.
	Dir string
	// Stream names the file, and is validated because it reaches the filesystem.
	Stream string
	// MaxBytes rotates the file once it grows past this size, so a persistent
	// failure cannot fill the volume.
	MaxBytes int64
	// IncludePayload records document bodies. Off by default.
	IncludePayload bool
	// Now is injected for tests.
	Now func() time.Time
}

// Writer appends records to a per-stream file.
type Writer struct {
	opts Options
	path string

	mu   sync.Mutex
	file *os.File
	size int64
}

// defaultMaxBytes rotates at a size that stays convenient to inspect by hand.
const defaultMaxBytes = 64 << 20

// New validates options and prepares a writer. The file is created on first use.
func New(opts Options) (*Writer, error) {
	if opts.Dir == "" {
		return nil, errors.New("dlq: a directory is required")
	}
	if opts.Stream == "" {
		return nil, errors.New("dlq: a stream name is required")
	}
	// The stream name becomes a filename, so anything that could escape the
	// directory is refused rather than sanitised.
	if opts.Stream != filepath.Base(opts.Stream) ||
		strings.ContainsAny(opts.Stream, `/\`) ||
		opts.Stream == "." || opts.Stream == ".." {
		return nil, fmt.Errorf("dlq: stream name %q cannot be used as a filename", opts.Stream)
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = defaultMaxBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Writer{opts: opts, path: filepath.Join(opts.Dir, opts.Stream+".jsonl")}, nil
}

// Path returns the file records are appended to.
func (w *Writer) Path() string { return w.path }

// Record appends one line per rejection and returns only once they are on disk.
func (w *Writer) Record(_ context.Context, rejections []sink.Rejection) error {
	if len(rejections) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.open(); err != nil {
		return err
	}

	buf := bufio.NewWriter(w.file)
	written := int64(0)
	for _, rej := range rejections {
		line, err := json.Marshal(w.toRecord(rej))
		if err != nil {
			return fmt.Errorf("dlq: encode record for %s: %w", rej.Doc.Key, err)
		}
		line = append(line, '\n')
		if _, err := buf.Write(line); err != nil {
			return fmt.Errorf("dlq: write record for %s: %w", rej.Doc.Key, err)
		}
		written += int64(len(line))
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("dlq: flush %s: %w", w.path, err)
	}

	// The caller advances its position after this returns, so the data has to be on
	// the device rather than in the operating system's cache.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("dlq: sync %s: %w", w.path, err)
	}
	w.size += written

	if w.size >= w.opts.MaxBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) toRecord(rej sink.Rejection) Record {
	rec := Record{
		RecordedAt: w.opts.Now().UTC(),
		Stream:     w.opts.Stream,
		Key:        rej.Doc.Key,
		Version:    rej.Doc.Version,
		Deleted:    rej.Doc.Deleted,
		Status:     rej.Status,
		Reason:     rej.Reason,
		BodyBytes:  len(rej.Doc.Body),
	}
	if w.opts.IncludePayload && len(rej.Doc.Body) > 0 {
		rec.Body = json.RawMessage(rej.Doc.Body)
	}
	return rec
}

// open creates the directory and file on first use, and recovers the current size
// so an existing file is appended to rather than truncated.
func (w *Writer) open() error {
	if w.file != nil {
		return nil
	}
	if err := os.MkdirAll(w.opts.Dir, 0o750); err != nil {
		return fmt.Errorf("dlq: create %s: %w", w.opts.Dir, err)
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("dlq: open %s: %w", w.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("dlq: stat %s: %w", w.path, err)
	}

	w.file, w.size = f, info.Size()
	return nil
}

// rotate renames the current file and starts a new one. Records are never
// discarded; keeping them is the reason the file exists.
func (w *Writer) rotate() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("dlq: close %s: %w", w.path, err)
	}
	w.file = nil

	stamp := w.opts.Now().UTC().Format("20060102T150405.000000000")
	rotated := fmt.Sprintf("%s.%s", w.path, stamp)
	if err := os.Rename(w.path, rotated); err != nil {
		return fmt.Errorf("dlq: rotate %s: %w", w.path, err)
	}
	w.size = 0
	return nil
}

// Close flushes and releases the file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// Read loads every record from a file.
//
// A line that cannot be parsed is an error rather than a skip: these records exist
// because something was already lost once, and quietly ignoring one would lose it
// again.
func Read(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dlq: open %s: %w", path, err)
	}
	defer f.Close()

	var (
		records []Record
		scanner = bufio.NewScanner(f)
		lineNo  int
	)
	// Records can be large when payloads are included.
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec Record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("dlq: %s line %d is not a valid record: %w", path, lineNo, err)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("dlq: read %s: %w", path, err)
	}
	return records, nil
}

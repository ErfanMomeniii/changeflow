package preflight

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/erfanmomeniii/changeflow/internal/source"
)

// CheckServer connects to the server, runs every check, prints the report to w, and returns it.
func CheckServer(ctx context.Context, dsn string, w io.Writer) (Report, error) {
	db, err := source.Open(ctx, dsn)
	if err != nil {
		return Report{}, err
	}
	defer db.Close()
	report, err := Run(ctx, DBReader{DB: db})
	if err != nil {
		return Report{}, err
	}
	report.Print(w)
	return report, nil
}

// Print renders a report for an operator.
func (r Report) Print(w io.Writer) {
	for _, c := range r.Checks {
		mark := "✓"
		if !c.OK {
			mark = "✗"
		}
		fmt.Fprintf(w, "%s %-28s want=%-10s got=%s\n", mark, c.Name, c.Want, c.Got)
	}
	for _, c := range r.Warnings() {
		fmt.Fprintf(w, "\nwarning: %s (%s)\n  %s\n", c.Name, c.Severity, c.Why)
	}
	for _, c := range r.Failures() {
		fmt.Fprintf(w, "\nFAIL: %s want=%s got=%s\n  %s\n", c.Name, c.Want, c.Got, c.Why)
	}
	fmt.Fprintln(w)
}

// ServerIDConflict reports whether the replica id an operator picked is the source's own.
func (r Report) ServerIDConflict(serverID uint32) bool {
	id, ok := r.Get("server_id")
	return ok && id == strconv.FormatUint(uint64(serverID), 10)
}

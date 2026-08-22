package supervisor

// Streaming: one reader on the source, fanning each change out to a pipeline per stream.

import (
	"context"
	"errors"
	"fmt"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/source/binlog"
)

// stream reads the source once and fans each change out to the streams that want it.
func (s *Supervisor) stream(
	ctx context.Context,
	sess *session,
	runtimes []*streamRuntime,
	positions map[string]string,
) error {
	// The shared position must be one no stream has passed, or a stream behind the others
	// would never receive the changes it still needs.
	startGTID, err := sharedStartPosition(positions)
	if err != nil {
		return err
	}

	router := NewRouter()
	for _, rt := range runtimes {
		router.Add(rt.cfg.Table, rt.events)
	}

	streamer, err := s.newStreamer(sess, router, startGTID, runtimes[0].alloc)
	if err != nil {
		return err
	}
	defer streamer.Close()

	s.log.Info("streaming started",
		"streams", streamNames(runtimes), "tables", router.Tables(),
		"from", startGTID, "server_id", s.cfg.Source.ServerID)

	group, groupCtx := newGroup(ctx)
	for _, rt := range runtimes {
		s.startPipeline(groupCtx, group, sess.store, rt)
	}
	s.startReader(groupCtx, group, sess.store, streamer, router, runtimes)

	err = group.wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// newStreamer opens the one binlog connection every stream reads through.
//
// One reader, so one sequencer. Versions need only increase within a key, so streams drawing
// from a shared sequence is not a problem.
func (s *Supervisor) newStreamer(
	sess *session,
	router *Router,
	startGTID string,
	seq binlog.Sequencer,
) (*binlog.Streamer, error) {
	host, port, err := addressOf(s.cfg.Source.DSN)
	if err != nil {
		return nil, err
	}

	return binlog.New(binlog.Options{
		Host:            host,
		Port:            port,
		User:            usernameOf(s.cfg.Source.DSN),
		Password:        passwordOf(s.cfg.Source.DSN),
		ServerID:        s.cfg.Source.ServerID,
		StartGTID:       startGTID,
		Tables:          router.Tables(),
		Schemas:         sess.schemas,
		Sequencer:       seq,
		HeartbeatPeriod: s.cfg.Source.HeartbeatPeriod.Duration(),
		ReadTimeout:     s.cfg.Source.ReadTimeout.Duration(),
		Buffer:          s.cfg.Runtime.BufferSize,
		Logger:          s.log,
	})
}

// startPipeline runs one stream's pipeline over its own queue, and samples that queue's
// depth alongside it.
func (s *Supervisor) startPipeline(
	ctx context.Context,
	group *group,
	store *checkpoint.MySQLStore,
	rt *streamRuntime,
) {
	rt.state.set(true, nil)
	// A previous run's failure is no longer the current state, and leaving it would have
	// status reporting an error that has been resolved.
	s.recordError(ctx, store, rt.cfg.Name, nil)

	group.run(func() error {
		err := rt.runner.Run(ctx, rt.events)
		rt.state.set(false, err)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.recordError(ctx, store, rt.cfg.Name, err)
			return fmt.Errorf("stream %s: %w", rt.cfg.Name, err)
		}
		return nil
	})
	go s.reportQueueDepth(ctx, rt)
}

// startReader runs the source reader that feeds every pipeline.
func (s *Supervisor) startReader(
	ctx context.Context,
	group *group,
	store *checkpoint.MySQLStore,
	streamer *binlog.Streamer,
	router *Router,
	runtimes []*streamRuntime,
) {
	group.run(func() error {
		// Closing the queues is how each pipeline learns the source has ended and flushes
		// what it holds.
		defer router.Close()

		for ev := range streamer.Events(ctx) {
			if err := router.Route(ctx, ev); err != nil {
				return err
			}
		}
		if err := streamer.Err(); err != nil {
			// The reader serves every stream, so its failure is recorded against all of
			// them: each one has stopped, and each will be looked at on its own.
			for _, rt := range runtimes {
				s.recordError(ctx, store, rt.cfg.Name, err)
			}
			return fmt.Errorf("supervisor: source stopped: %w", err)
		}
		return nil
	})
}

func streamNames(runtimes []*streamRuntime) []string {
	names := make([]string, 0, len(runtimes))
	for _, rt := range runtimes {
		names = append(names, rt.cfg.Name)
	}
	return names
}

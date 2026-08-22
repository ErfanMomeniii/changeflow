package supervisor

import (
	"context"
	"errors"
	"fmt"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/source/binlog"
)

func (s *Supervisor) stream(
	ctx context.Context,
	sess *session,
	runtimes []*streamRuntime,
	positions map[string]string,
) error {
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

func (s *Supervisor) startPipeline(
	ctx context.Context,
	group *group,
	store *checkpoint.MySQLStore,
	rt *streamRuntime,
) {
	rt.state.set(true, nil)
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

func (s *Supervisor) startReader(
	ctx context.Context,
	group *group,
	store *checkpoint.MySQLStore,
	streamer *binlog.Streamer,
	router *Router,
	runtimes []*streamRuntime,
) {
	group.run(func() error {
		defer router.Close()
		for ev := range streamer.Events(ctx) {
			if err := router.Route(ctx, ev); err != nil {
				return err
			}
		}
		if err := streamer.Err(); err != nil {
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

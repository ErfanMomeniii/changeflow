package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// schemaRequest is what generating a destination schema needs: which stream, and the index
// settings that only Elasticsearch uses.
type schemaRequest struct {
	path     string
	stream   string
	shards   int
	replicas int
}

func newGenerateSchemaCmd() *cobra.Command {
	var req schemaRequest

	cmd := &cobra.Command{
		Use:   "generate-schema",
		Short: "Print the destination schema for a stream, to review and apply",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return generateSchema(cmd.Context(), req)
		},
	}

	configFlag(cmd, &req.path)
	cmd.Flags().StringVar(&req.stream, "stream", "", "which configured stream to generate for")
	cmd.Flags().IntVar(&req.shards, "shards", 1, "number_of_shards for a generated Elasticsearch index")
	cmd.Flags().IntVar(&req.replicas, "replicas", 1, "number_of_replicas for a generated Elasticsearch index")
	_ = cmd.MarkFlagRequired("stream")
	return cmd
}

// generateSchema prints the destination schema for a stream.
//
// It is deliberately a printer rather than an applier: changeflow never issues DDL to
// a destination. The output is committed and applied by whatever migration tooling the
// project already uses, so a schema change is reviewed like any other change.
func generateSchema(ctx context.Context, req schemaRequest) error {
	cfg, err := config.Load(req.path)
	if err != nil {
		return err
	}
	stream, err := cfg.Stream(req.stream)
	if err != nil {
		return err
	}

	db, err := open(ctx, cfg.Source.DSN)
	if err != nil {
		return err
	}
	defer db.Close()

	// Read from the live source, so what is generated matches what will be replicated
	// rather than what someone remembers the table looking like.
	meta, err := schema.DBLoader{DB: db}.Load(ctx, stream.Schema(), stream.TableName())
	if err != nil {
		return err
	}
	key, err := meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return err
	}

	var generated schema.Generated
	switch stream.Sink.Type {
	case config.SinkElasticsearch:
		generated, err = schema.GenerateElasticsearch(meta,
			stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename,
			req.shards, req.replicas)
	case config.SinkClickHouse:
		generated, err = schema.GenerateClickHouse(meta,
			stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename,
			stream.Sink.Table)
	default:
		return fmt.Errorf("no schema generator for sink type %q", stream.Sink.Type)
	}
	if err != nil {
		return err
	}

	// The schema goes to stdout so it can be redirected into a file, while the notes
	// go to stderr so they do not end up inside it.
	fmt.Print(generated.Body)
	for _, warning := range generated.Warnings {
		fmt.Fprintf(os.Stderr, "note: %s\n", warning)
	}
	return nil
}

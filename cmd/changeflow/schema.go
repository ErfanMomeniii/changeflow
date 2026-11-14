package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erfanmomeniii/changeflow/internal/config"
	"github.com/erfanmomeniii/changeflow/internal/supervisor"
)

func newGenerateSchemaCmd() *cobra.Command {
	var (
		path     string
		stream   string
		shards   int
		replicas int
	)
	cmd := &cobra.Command{
		Use:   "generate-schema",
		Short: "Print the destination schema for a stream, to review and apply",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			generated, err := supervisor.GenerateSchema(cmd.Context(), cfg, stream, shards, replicas)
			if err != nil {
				return err
			}
			fmt.Print(generated.Body)
			for _, warning := range generated.Warnings {
				fmt.Fprintf(os.Stderr, "note: %s\n", warning)
			}
			return nil
		},
	}
	configFlag(cmd, &path)
	cmd.Flags().StringVar(&stream, "stream", "", "which configured stream to generate for")
	cmd.Flags().IntVar(&shards, "shards", 1, "number_of_shards for a generated Elasticsearch index")
	cmd.Flags().IntVar(&replicas, "replicas", 1, "number_of_replicas for a generated Elasticsearch index")
	_ = cmd.MarkFlagRequired("stream")
	return cmd
}

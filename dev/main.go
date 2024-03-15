// Utilities to develop the clickhouse module

package main

import "context"

type Dev struct{}

// Return a pinned ref for the latest version of the base image.
// By hardcoding the full ref into the clickhouse module's source code,
// we make it faster by saving the overhead of a tag lookup.'
func (m *Dev) Pin(ctx context.Context) (string, error) {
	return dag.Container().From(
		"index.docker.io/clickhouse/clickhouse-server",
	).ImageRef(
		ctx,
	)
}

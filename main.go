// A Dagger module to integrate with Clickhouse
//
// A Dagger module to integrate with Clickhouse
// https://clickhouse.com

package main

import (
	"context"
	"fmt"

	"mvdan.cc/sh/v3/syntax"
)

type Clickhouse struct {
	Host     string
	Port     int
	User     string
	Password *Secret
	// +private
	ShellCommand string
}

func New(
	ctx context.Context,
	// Clickhouse hostname
	host string,
	port int,
	user string,
	password *Secret,
) (*Clickhouse, error) {
	m := &Clickhouse{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	}
	cmd, err := m.shellCommand(ctx)
	if err != nil {
		return m, err
	}
	m.ShellCommand = cmd
	return m, nil
}

// Returns a container that echoes whatever string argument is provided
func (m *Clickhouse) Container() *Container {
	return dag.
		Container().
		From("index.docker.io/clickhouse/clickhouse-server").
		WithNewFile("/root/.bash_history", ContainerWithNewFileOpts{Contents: m.ShellCommand + "\n"}).
		WithDefaultTerminalCmd([]string{"su", "-"})
}

func squotef(s string, args ...string) (string, error) {
	quoted := make([]interface{}, len(args))
	for i := range args {
		s, err := syntax.Quote(args[i], syntax.LangBash)
		if err != nil {
			return "", err
		}
		quoted[i] = s
	}
	return fmt.Sprintf(s, quoted...), nil
}

func (m *Clickhouse) shellCommand(ctx context.Context) (string, error) {
	// Password value has to be passed in arguments,
	//  so we wrap the command in a shell script to avoid leaks
	pw, err := m.Password.Plaintext(ctx)
	if err != nil {
		return "", err
	}
	return squotef(
		`clickhouse client --host %s --port %s --user %s --password %s --secure --format CSV`,
		m.Host,
		fmt.Sprintf("%d", m.Port),
		m.User,
		pw,
	)
}

func (m *Clickhouse) CSV(q string) *File {
	return m.
		Container().
		WithExec([]string{"sh", "-c", m.ShellCommand},
			ContainerWithExecOpts{
				Stdin:          q,
				RedirectStdout: "out.csv",
			}).
		File("out.csv")
}

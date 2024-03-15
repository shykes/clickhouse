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

const (
	// Generated with `dagger call -m ./dev pin`
	baseImage = `docker.io/clickhouse/clickhouse-server@sha256:2935c2d30c49117a979a1bacd513423d0c339c933cfdfd8a7a99c23af6a7cdf3`
)

// A clickhouse client configuration
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
	// Clickhouse port
	// +default=9000
	port int,
	// Clickhouse user
	user string,
	// Clickhouse password
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
		From(baseImage).
		WithNewFile("/root/.bash_history", ContainerWithNewFileOpts{Contents: m.ShellCommand + "\n"}).
		WithDefaultTerminalCmd([]string{"su", "-"})
}

// Like Sprintg, but all arguments are quoted for bash
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

// The clickhouse client command, fully configured, bash-ready
// WARNING: this includes the cleartext of the clickhouse password
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

// Send a SQL query, and return the result as a CSV file
func (m *Clickhouse) CSV(
	// The SQL query
	query string,
) *File {
	return m.
		Container().
		WithExec([]string{"sh", "-c", m.ShellCommand},
			ContainerWithExecOpts{
				Stdin:          query,
				RedirectStdout: "out.csv",
			}).
		File("out.csv")
}

// Package pgconn is a low-level PostgreSQL database driver that operates at
// nearly the same level as the wire protocol.
package pgconn

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/11/static/protocol-error-fields.html for
// detailed field description.
type PgError struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	ConstraintName   string
	File             string
	Line             int32
	Routine          string
}

func (pe *PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}

// ConnConfig contains all the options used to establish a connection. It must
// be created by ParseConfig and then it can be modified.
type ConnConfig struct {
	Host           string
	Port           uint16
	Database       string
	User           string
	Password       string
	TLSConfig      *tls.Config // nil disables TLS
	DialFunc       DialFunc
	ConnectTimeout time.Duration
	RuntimeParams  map[string]string
}

// DialFunc is a function that can be used to connect to a PostgreSQL server.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// PgConn is a low-level PostgreSQL connection handle. It is not safe for
// concurrent usage.
type PgConn struct {
	conn          net.Conn
	config        *ConnConfig
	pid           uint32 // backend pid
	secretKey     uint32 // key to use to send a cancel query message to the server
	parameterStatuses map[string]string
	txStatus      byte
	closed        bool
}

// Connect establishes a connection to a PostgreSQL server using the provided
// configuration. ctx can be used to cancel a connect attempt.
func Connect(ctx context.Context, config *ConnConfig) (*PgConn, error) {
	if config == nil {
		return nil, fmt.Errorf("config must be provided")
	}

	pc := &PgConn{
		config:            config,
		parameterStatuses: make(map[string]string),
	}

	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	network := "tcp"

	var dialFunc DialFunc
	if config.DialFunc != nil {
		dialFunc = config.DialFunc
	} else {
		var d net.Dialer
		dialFunc = d.DialContext
	}

	var err error
	pc.conn, err = dialFunc(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("dial error: %w", err)
	}

	if config.TLSConfig != nil {
		pc.conn = tls.Client(pc.conn, config.TLSConfig)
	}

	return pc, nil
}

// Close closes the connection to the database.
func (pc *PgConn) Close(ctx context.Context) error {
	if pc.closed {
		return nil
	}
	pc.closed = true
	return pc.conn.Close()
}

// IsClosed reports whether the connection has been closed.
func (pc *PgConn) IsClosed() bool {
	return pc.closed
}

// ParameterStatus returns the value of a parameter reported by the server
// (e.g. server_version, client_encoding). Returns an empty string for
// unknown parameters.
func (pc *PgConn) ParameterStatus(key string) string {
	return pc.parameterStatuses[key]
}

// PID returns the backend PID for this connection.
func (pc *PgConn) PID() uint32 {
	return pc.pid
}

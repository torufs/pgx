package pgconn

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the settings used to establish a connection to a PostgreSQL server.
type Config struct {
	Host           string // host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
	Port           uint16
	Database       string
	User           string
	Password       string
	TLSConfig      *tls.Config // nil disables TLS
	ConnectTimeout time.Duration
	DialFunc       DialFunc
	LookupFunc     LookupFunc
	BuildFrontend  BuildFrontendFunc
	RuntimeParams  map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)

	Fallbacks []*FallbackConfig

	// ValidateConnect is called during a connection attempt after a successful authentication with the PostgreSQL server.
	// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
	// fallback or error is tried. This allows implementing high availability behavior such as libpq does with
	// target_session_attrs.
	ValidateConnect ValidateConnectFunc

	// AfterConnect is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
	// or prepare statements). If this returns an error the connection attempt fails.
	AfterConnect AfterConnectFunc

	// OnNotice is a callback function called when a notice response is received.
	OnNotice NoticeHandler

	// OnNotification is a callback function called when a notification from the LISTEN/NOTIFY system is received.
	OnNotification NotificationHandler
}

// FallbackConfig is additional settings to attempt a connection with when the primary Config fails to establish a
// network connection. It is used for TLS fallback such as sslmode=prefer where if a TLS connection fails a non-TLS
// connection should be attempted.
type FallbackConfig struct {
	Host      string
	Port      uint16
	TLSConfig *tls.Config // nil disables TLS
}

// ParseConfig parses connString into a *Config. connString may be a DSN or a URL.
//
// Example DSN:
//
//	host=localhost port=5432 dbname=mydb user=jack password=secret sslmode=prefer
//
// Example URL:
//
//	postgres://jack:secret@localhost:5432/mydb?sslmode=prefer
func ParseConfig(connString string) (*Config, error) {
	var parseErr error
	var settings map[string]string

	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		settings, parseErr = parseURLSettings(connString)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse connection URL: %w", parseErr)
		}
	} else {
		settings = parseDSNSettings(connString)
	}

	config := &Config{
		Host:          getEnvOrDefault("PGHOST", settings["host"], "localhost"),
		Database:      getEnvOrDefault("PGDATABASE", settings["dbname"], ""),
		User:          getEnvOrDefault("PGUSER", settings["user"], os.Getenv("USER")),
		Password:      getEnvOrDefault("PGPASSWORD", settings["password"], ""),
		RuntimeParams: make(map[string]string),
	}

	if portStr := getEnvOrDefault("PGPORT", settings["port"], "5432"); portStr != "" {
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
		}
		config.Port = uint16(port)
	}

	return config, nil
}

// parseURLSettings parses a connection URL into a settings map.
func parseURLSettings(connString string) (map[string]string, error) {
	settings := make(map[string]string)

	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}

	if u.User != nil {
		settings["user"] = u.User.Username()
		if password, ok := u.User.Password(); ok {
			settings["password"] = password
		}
	}

	if host, port, err := splitHostPort(u.Host); err == nil {
		settings["host"] = host
		if port != "" {
			settings["port"] = port
		}
	} else {
		settings["host"] = u.Host
	}

	if u.Path != "" {
		settings["dbname"] = strings.TrimPrefix(u.Path, "/")
	}

	for k, v := range u.Query() {
		if len(v) > 0 {
			settings[k] = v[0]
		}
	}

	return settings, nil
}

// parseDSNSettings parses a DSN string into a settings map.
func parseDSNSettings(dsn string) map[string]string {
	settings := make(map[string]string)
	for _, part := range strings.Fields(dsn) {
		if idx := strings.IndexByte(part, '='); idx >= 0 {
			key := strings.TrimSpace(part[:idx])
			val := strings.TrimSpace(part[idx+1:])
			settings[key] = val
		}
	}
	return settings
}

func splitHostPort(hostport string) (host, port string, err error) {
	if idx := strings.LastIndexByte(hostport, ':'); idx >= 0 {
		return hostport[:idx], hostport[idx+1:], nil
	}
	return "", "", fmt.Errorf("missing port in address")
}

func getEnvOrDefault(envVar, explicit, fallback string) string {
	if explicit != "" {
		return explicit
	}
	if val := os.Getenv(envVar); val != "" {
		return val
	}
	return fallback
}

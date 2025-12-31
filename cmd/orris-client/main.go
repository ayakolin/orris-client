package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/orris-inc/orris-client/internal/agent"
	"github.com/orris-inc/orris-client/internal/config"
	"github.com/orris-inc/orris-client/internal/logger"
	"github.com/orris-inc/orris-client/internal/version"
	flag "github.com/spf13/pflag"
)

func main() {
	var (
		serverURL     = flag.StringP("server", "s", "", "server URL")
		token         = flag.StringP("token", "t", "", "bearer token")
		wsListenPort  = flag.Uint16P("ws-port", "W", 0, "WebSocket listen port for tunnel (0 = random)")
		tlsListenPort = flag.Uint16P("tls-port", "T", 0, "TLS listen port for tunnel (0 = random)")
		logLevel      = flag.StringP("loglevel", "l", "info", "log level (debug, info, warn, error)")
		showVersion   = flag.BoolP("version", "v", false, "show version and exit")
	)
	flag.Parse()

	// Get log level: command line flag takes precedence over environment variable
	level := *logLevel
	if !flag.CommandLine.Changed("loglevel") {
		if envLevel := os.Getenv("ORRIS_LOG_LEVEL"); envLevel != "" {
			level = envLevel
		}
	}

	// Set log level
	switch strings.ToLower(level) {
	case "debug":
		logger.SetLevel(slog.LevelDebug)
	case "info":
		logger.SetLevel(slog.LevelInfo)
	case "warn":
		logger.SetLevel(slog.LevelWarn)
	case "error":
		logger.SetLevel(slog.LevelError)
	}

	if *showVersion {
		fmt.Printf("orris-client %s (commit: %s, built: %s)\n", version.Version, version.Commit, version.BuildTime)
		os.Exit(0)
	}

	cfg := config.LoadFromEnv()

	if *serverURL != "" {
		cfg.ServerURL = *serverURL
	}
	if *token != "" {
		cfg.Token = *token
	}
	if *wsListenPort != 0 {
		cfg.WsListenPort = *wsListenPort
	}
	if *tlsListenPort != 0 {
		cfg.TlsListenPort = *tlsListenPort
	}

	if cfg.Token == "" {
		logger.Error("token is required (use -t/--token or ORRIS_TOKEN env)")
		os.Exit(1)
	}

	logger.Info("starting orris-client", "version", version.Version, "server", cfg.ServerURL)

	ag := agent.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ag.Start(ctx); err != nil {
		logger.Error("failed to start agent", "error", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("shutting down")

	ag.Stop()
	logger.Info("agent stopped")
}

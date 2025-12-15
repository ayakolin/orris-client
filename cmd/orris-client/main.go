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
	flag "github.com/spf13/pflag"
)

// Build-time variables injected via ldflags
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	var (
		serverURL    = flag.StringP("server", "u", "", "server URL")
		token        = flag.StringP("token", "t", "", "bearer token")
		wsListenPort = flag.Uint16P("ws-port", "w", 0, "WebSocket listen port for tunnel (0 = random)")
		logLevel     = flag.StringP("loglevel", "l", "info", "log level (debug, info, warn, error)")
		showVersion  = flag.BoolP("version", "v", false, "show version and exit")
	)
	flag.Parse()

	// Set log level
	switch strings.ToLower(*logLevel) {
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
		fmt.Printf("orris-client %s (commit: %s, built: %s)\n", version, commit, buildTime)
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

	if cfg.Token == "" {
		logger.Error("token is required (use --token or ORRIS_TOKEN env)")
		os.Exit(1)
	}

	logger.Info("starting orris-client", "version", version, "server", cfg.ServerURL)

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

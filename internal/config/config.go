package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServerURL       string
	Token           string
	WsListenPort    uint16 // WebSocket listen port for tunnel connections (exit agent)
	SyncInterval    time.Duration
	TrafficInterval time.Duration
	StatusInterval  time.Duration
	HTTPTimeout     time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ServerURL:       "http://localhost:8080",
		Token:           "",
		SyncInterval:    30 * time.Second,
		TrafficInterval: 60 * time.Second,
		StatusInterval:  30 * time.Second,
		HTTPTimeout:     10 * time.Second,
	}
}

func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if v := os.Getenv("ORRIS_SERVER_URL"); v != "" {
		cfg.ServerURL = v
	}
	if v := os.Getenv("ORRIS_TOKEN"); v != "" {
		cfg.Token = v
	}
	if v := os.Getenv("ORRIS_WS_LISTEN_PORT"); v != "" {
		if port, err := strconv.ParseUint(v, 10, 16); err == nil {
			cfg.WsListenPort = uint16(port)
		}
	}

	return cfg
}

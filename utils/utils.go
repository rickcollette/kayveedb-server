package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// Config holds the server configuration parameters.
type Config struct {
	DatabasePath string   `json:"database_path"`
	ListenCIDR   []string `json:"listen_cidr"`
	TCPPort      string   `json:"tcp_port"`
	CacheSize    int      `json:"cache_size"`
}

// LoadConfig loads the configuration from the appropriate file.
func LoadConfig() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		// Determine default config path based on OS
		if runtime.GOOS == "windows" {
			appData := os.Getenv("APPDATA")
			if appData == "" {
				return nil, fmt.Errorf("APPDATA environment variable not set on Windows")
			}
			configPath = filepath.Join(appData, "KayVeeDB", "kayveedb.conf")
		} else {
			// Linux/MacOS
			configPath = "/etc/kayveedb/kayveedb.conf"
		}
	}

	// Read the config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file at %s: %w", configPath, err)
	}

	// Parse the JSON config
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate required fields
	if config.DatabasePath == "" {
		return nil, fmt.Errorf("database_path is required in config file")
	}
	if config.TCPPort == "" {
		return nil, fmt.Errorf("tcp_port is required in config file")
	}
	if config.CacheSize <= 0 {
		return nil, fmt.Errorf("cache_size must be a positive integer")
	}

	return &config, nil
}

// LoadSecrets loads the required secrets from environment variables.
// Returns an error if any required secret is missing.
func LoadSecrets() (hmacKey, encryptionKey, nonce []byte, err error) {
	hmacKeyStr := os.Getenv("HMAC_KEY")
	encryptionKeyStr := os.Getenv("ENCRYPTION_KEY")
	nonceStr := os.Getenv("NONCE")

	if hmacKeyStr == "" {
		err = fmt.Errorf("HMAC_KEY environment variable is not set")
		return
	}
	if encryptionKeyStr == "" {
		err = fmt.Errorf("ENCRYPTION_KEY environment variable is not set")
		return
	}
	if nonceStr == "" {
		err = fmt.Errorf("NONCE environment variable is not set")
		return
	}

	hmacKey = []byte(hmacKeyStr)
	encryptionKey = []byte(encryptionKeyStr)
	nonce = []byte(nonceStr)

	// Validate encryption key length (32 bytes for XChaCha20)
	if len(encryptionKey) != 32 {
		err = fmt.Errorf("ENCRYPTION_KEY must be exactly 32 bytes")
		return
	}

	// Validate nonce length (24 bytes for XChaCha20)
	if len(nonce) != 24 {
		err = fmt.Errorf("NONCE must be exactly 24 bytes")
		return
	}

	return
}

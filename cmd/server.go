package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rickcollette/kayveedb-server/core"
	"github.com/rickcollette/kayveedb-server/daemon"
	"github.com/rickcollette/kayveedb-server/utils"
	"github.com/spf13/cobra"
)

// StartCmd starts the KayVeeDB server.
func StartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the KayVeeDB server",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load configuration
			cfg, err := utils.LoadConfig()
			if err != nil {
				return fmt.Errorf("failed to load config: %v", err)
			}

			// Load secrets
			hmacKey, encryptionKey, nonce, err := utils.LoadSecrets()
			if err != nil {
				return fmt.Errorf("failed to load secrets: %v", err)
			}

			// Initialize core with configuration and secrets
			sec := &core.Secrets{
				HMACKey:       hmacKey,
				EncryptionKey: encryptionKey,
				Nonce:         nonce,
			}

			if err := core.Initialize(cfg, sec); err != nil {
				return fmt.Errorf("failed to initialize core: %v", err)
			}
			defer func() {
				if err := core.Shutdown(); err != nil {
					log.Printf("Error during shutdown: %v\n", err)
				}
			}()

			// Setup signal handling for graceful shutdown
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

			// Start the daemon in a separate goroutine
			errChan := make(chan error, 1)
			go func() {
				errChan <- daemon.StartDaemon(cfg.TCPPort)
			}()

			log.Println("Starting server on port:", cfg.TCPPort)

			// Wait for a termination signal or daemon error
			select {
			case sig := <-sigs:
				log.Printf("Received signal %s. Shutting down...\n", sig)
				return nil
			case err := <-errChan:
				return fmt.Errorf("daemon error: %v", err)
			}
		},
	}
}

func init() {
	// No default flags as configuration is loaded from files/environment variables
}

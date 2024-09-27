package core

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/rickcollette/kayveedb"
	"github.com/rickcollette/kayveedb/lib"
)

// Record represents a simple key-value record in the database.
type Record struct {
	Key   string
	Value string
}

var (
	// databases holds the active B-trees indexed by database name.
	databases = make(map[string]*kayveedb.BTree)
	dbMutex   sync.RWMutex
	config    *Config
	secrets   *Secrets

	// Managers
	authManager        *lib.AuthManager
	transactionManager *lib.TransactionManager
	cacheManager       *lib.CacheManager
	pubSub             *lib.PubSub
	listManager        *lib.ListManager
	setManager         *lib.SetManager
	hashManager        *lib.HashManager
	zsetManager        *lib.ZSetManager
)

// Config holds the server configuration parameters.
type Config struct {
	DatabasePath string
	ListenCIDR   []string
	TCPPort      string
	CacheSize    int
}

// Secrets holds the sensitive information required for encryption.
type Secrets struct {
	HMACKey       []byte
	EncryptionKey []byte
	Nonce         []byte
}

// Initialize initializes the KayVeeDB B-trees and all managers based on the configuration.
func Initialize(cfg *Config, sec *Secrets) error {
	config = cfg
	secrets = sec

	// Initialize AuthManager
	authManager = lib.NewAuthManager()

	// Optionally, create a default admin user
	if err := authManager.CreateUser("admin", "password"); err != nil {
		return fmt.Errorf("failed to create default admin user: %w", err)
	}

	// Initialize TransactionManager
	transactionManager = lib.NewTransactionManager()

	// Initialize CacheManager with a flush function
	cacheManager = lib.NewCacheManager(cfg.CacheSize, flushCacheToDisk)

	// Initialize PubSub
	pubSub = lib.NewPubSub()

	// Initialize Data Structure Managers
	listManager = lib.NewListManager()
	setManager = lib.NewSetManager()
	hashManager = lib.NewHashManager()
	zsetManager = lib.NewZSetManager()

	// Ensure the main log file exists
	mainLogPath := filepath.Join(config.DatabasePath, "kayveedb.log")
	if _, err := os.Stat(mainLogPath); os.IsNotExist(err) {
		file, err := os.Create(mainLogPath)
		if err != nil {
			return fmt.Errorf("failed to create main log file: %w", err)
		}
		file.Close()
	}

	// Scan the DATABASE_PATH for existing databases
	entries, err := os.ReadDir(config.DatabasePath)
	if err != nil {
		return fmt.Errorf("failed to read database path: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			dbName := entry.Name()
			if dbName == "." || dbName == ".." {
				continue
			}
			if err := loadDatabase(dbName); err != nil {
				log.Printf("Failed to load database '%s': %v", dbName, err)
			}
		}
	}

	return nil
}

// loadDatabase initializes a B-tree for the specified database name.
func loadDatabase(dbName string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if _, exists := databases[dbName]; exists {
		return fmt.Errorf("database '%s' already loaded", dbName)
	}

	dbPath := filepath.Join(config.DatabasePath, dbName)

	// Ensure the database directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize the B-tree
	btree, err := kayveedb.NewBTree(
		3,                              // B-tree order 't'
		dbPath,                         // Database path
		fmt.Sprintf("%s.kvdb", dbName), // dbName
		fmt.Sprintf("%s.log", dbName),  // logName
		secrets.HMACKey,
		secrets.EncryptionKey,
		secrets.Nonce,
		config.CacheSize,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize B-tree: %w", err)
	}

	// Add to the databases map
	databases[dbName] = btree
	log.Printf("Database '%s' loaded successfully.", dbName)
	return nil
}

// Shutdown gracefully shuts down all components.
func Shutdown() error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	for dbName, btree := range databases {
		if err := btree.Shutdown(); err != nil {
			log.Printf("Failed to shutdown database '%s': %v", dbName, err)
		} else {
			log.Printf("Database '%s' shutdown successfully.", dbName)
		}
		delete(databases, dbName)
	}

	// Additional shutdown steps if necessary
	// e.g., closing pubSub channels

	return nil
}

// InsertKey inserts or updates a key-value pair in the specified database.
func InsertKey(dbName, key, value string) error {
	btree, err := getDatabase(dbName)
	if err != nil {
		return err
	}

	encValue := []byte(value) // Convert string to bytes

	if err := btree.Insert(key, encValue, secrets.EncryptionKey, secrets.Nonce); err != nil {
		log.Printf("InsertKey error: %v", err)
		return err
	}

	return nil
}

// UpdateKey updates the value for a specific key in the specified database.
func UpdateKey(dbName, key, value string) error {
	btree, err := getDatabase(dbName)
	if err != nil {
		return err
	}

	encValue := []byte(value) // Convert string to bytes

	// Check if the key exists
	_, err = btree.Read(key, secrets.EncryptionKey, secrets.Nonce)
	if err != nil {
		log.Printf("UpdateKey read error: %v", err)
		return errors.New("key not found")
	}

	if err := btree.Insert(key, encValue, secrets.EncryptionKey, secrets.Nonce); err != nil {
		log.Printf("UpdateKey insert error: %v", err)
		return err
	}

	return nil
}

// DeleteKey deletes a key from the specified database.
func DeleteKey(dbName, key string) error {
	btree, err := getDatabase(dbName)
	if err != nil {
		return err
	}

	err = btree.Delete(key)
	if err != nil {
		return fmt.Errorf("delete failed: %v", err)
	}

	return nil
}

// ReadKey reads the value for a specific key from the specified database.
func ReadKey(dbName, key string) (string, error) {
	btree, err := getDatabase(dbName)
	if err != nil {
		return "", err
	}

	data, err := btree.Read(key, secrets.EncryptionKey, secrets.Nonce)
	if err != nil {
		log.Printf("ReadKey error: %v", err)
		return "", err
	}

	return string(data), nil
}

// ListKeys returns a slice of all keys in the specified database.
func ListKeys(dbName string) ([]string, error) {
	btree, err := getDatabase(dbName)
	if err != nil {
		return nil, err
	}

	keys, err := btree.ListKeys()
	if err != nil {
		log.Printf("ListKeys error: %v", err)
		return nil, err
	}

	return keys, nil
}

// CreateDatabase creates a new database with the specified name.
func CreateDatabase(dbName string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if _, exists := databases[dbName]; exists {
		return fmt.Errorf("database '%s' already exists", dbName)
	}

	dbPath := filepath.Join(config.DatabasePath, dbName)
	kvdbPath := filepath.Join(dbPath, fmt.Sprintf("%s.kvdb", dbName))
	logPath := filepath.Join(dbPath, fmt.Sprintf("%s.log", dbName))

	// Ensure the database directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize the B-tree
	btree, err := kayveedb.NewBTree(
		3,                              // B-tree order 't'
		dbPath,                         // Database path
		fmt.Sprintf("%s.kvdb", dbName), // dbName
		fmt.Sprintf("%s.log", dbName),  // logName
		secrets.HMACKey,
		secrets.EncryptionKey,
		secrets.Nonce,
		config.CacheSize,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize B-tree: %w", err)
	}

	// Add to the databases map
	databases[dbName] = btree
	log.Printf("Database '%s' created and loaded successfully.", dbName)
	return nil
}

// LoadDatabase attempts to load an existing database or creates a new one if it doesn't exist.
func LoadDatabase(dbName string) error {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if _, exists := databases[dbName]; exists {
		return nil // Already loaded
	}

	dbPath := filepath.Join(config.DatabasePath, dbName)
	kvdbPath := filepath.Join(dbPath, fmt.Sprintf("%s.kvdb", dbName))

	// Check if the database files exist
	if _, err := os.Stat(kvdbPath); os.IsNotExist(err) {
		// Create a new database if it doesn't exist
		if err := CreateDatabase(dbName); err != nil {
			return fmt.Errorf("failed to create database '%s': %w", dbName, err)
		}
	} else if err != nil {
		return fmt.Errorf("error checking database '%s': %w", dbName, err)
	} else {
		// Load existing database
		if err := loadDatabase(dbName); err != nil {
			return fmt.Errorf("failed to load database '%s': %w", dbName, err)
		}
	}

	return nil
}

// AuthenticateUser authenticates a user using AuthManager
func AuthenticateUser(username, password string) error {
	if err := authManager.Connect(username, password); err != nil {
		return err
	}
	return nil
}

// DisconnectUser disconnects a user session
func DisconnectUser(username string) {
	authManager.Disconnect(username)
}

// Transaction Functions

// BeginTransaction starts a new transaction for a client
func BeginTransaction(clientID uint32) {
	transactionManager.Begin(clientID)
}

// CommitTransaction commits the transaction for a client
func CommitTransaction(clientID uint32) error {
	return transactionManager.Commit(clientID)
}

// RollbackTransaction rolls back the transaction for a client
func RollbackTransaction(clientID uint32) error {
	return transactionManager.Rollback(clientID)
}

// Cache Functions

// SetCache adds a key-value pair to the cache
func SetCache(key string, value []byte) {
	cacheManager.SetCache(key, value)
}

// GetCache retrieves a value from the cache
func GetCache(key string) (*lib.Node, error) {
	node, err := cacheManager.GetCache(key)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// DeleteCache deletes a key from the cache
func DeleteCache(key string) error {
	return cacheManager.DeleteCache(key)
}

// FlushCache removes all entries from the cache
func FlushCache() {
	cacheManager.FlushCache()
}

// PubSub Functions

// PublishMessage publishes a message to a channel
func PublishMessage(channel, message string) {
	pubSub.Publish(channel, message)
}

// SubscribeChannel allows a client to subscribe to a channel
func SubscribeChannel(channel string) lib.Subscriber {
	return pubSub.Subscribe(channel)
}

// UnsubscribeChannel allows a client to unsubscribe from a channel
func UnsubscribeChannel(channel string, sub lib.Subscriber) {
	pubSub.Unsubscribe(channel, sub)
}

// Data Structure Functions

// List Operations

// LPush adds a value to the head of a list
func LPush(key, value string) {
	listManager.LPush(key, value)
}

// RPush adds a value to the tail of a list
func RPush(key, value string) {
	listManager.RPush(key, value)
}

// LRange retrieves a range of values from a list
func LRange(key string, start, stop int) ([]string, error) {
	return listManager.LRange(key, start, stop)
}

// Set Operations

// SAdd adds a member to a set
func SAdd(key, member string) {
	setManager.SAdd(key, member)
}

// SMembers retrieves all members of a set
func SMembers(key string) ([]string, error) {
	return setManager.SMembers(key)
}

// Hash Operations

// HSet sets a field in a hash
func HSet(key, field, value string) {
	hashManager.HSet(key, field, value)
}

// HGet retrieves a field from a hash
func HGet(key, field string) (string, error) {
	return hashManager.HGet(key, field)
}

// Sorted Set Operations

// ZAdd adds a member with a score to a sorted set
func ZAdd(key, member string, score float64) {
	zsetManager.ZAdd(key, member, score)
}

// ZRange retrieves a range of members from a sorted set
func ZRange(key string, start, stop int) ([]string, error) {
	return zsetManager.ZRange(key, start, stop)
}

// getDatabase retrieves the B-tree for the specified database name.
func getDatabase(dbName string) (*kayveedb.BTree, error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	btree, exists := databases[dbName]
	if !exists {
		return nil, fmt.Errorf("database '%s' is not loaded", dbName)
	}
	return btree, nil
}

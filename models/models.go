package models

import (
	"net"
	"sync"
	"time"

	"github.com/rickcollette/kayveedb/protocol"
)

// Subscriber alias for Pub/Sub
type Subscriber = protocol.Subscriber

// KeyValueDBSession holds basic session information.
type KeyValueDBSession struct {
	ID            string
	Authenticated bool
	SelectedDB    string
	LastActive    time.Time
	Conn          net.Conn
	Mutex         sync.Mutex
}

// Session represents a client session with additional features.
type Session struct {
	KeyValueDBSession
	SubscribedChannels []string
	Subscribers        []Subscriber
}

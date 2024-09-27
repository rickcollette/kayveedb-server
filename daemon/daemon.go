package daemon

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rickcollette/kayveedb-server/core"
	"github.com/rickcollette/kayveedb-server/models"
	"github.com/rickcollette/kayveedb/protocol"
)

// Session represents a client session
type Session struct {
	models.KeyValueDBSession
	SubscribedChannels []string
	Subscribers        []protocol.Subscriber
	Mutex              sync.Mutex
}

// StartDaemon starts the KayVeeDB server and listens for TLS connections.
func StartDaemon(port string) error {
	cert, err := tls.LoadX509KeyPair("certs/server.crt", "certs/server.key")
	if err != nil {
		return fmt.Errorf("error loading TLS certificate: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		// Uncomment for mutual TLS
		// ClientAuth: tls.RequireAndVerifyClientCert,
		// ClientCAs: loadClientCAs(), // If using mutual TLS
	}

	listener, err := tls.Listen("tcp", ":"+port, tlsConfig)
	if err != nil {
		return fmt.Errorf("error starting TLS server: %v", err)
	}
	defer listener.Close()

	log.Println("TLS Daemon listening on port", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting TLS connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	session := &Session{
		KeyValueDBSession: models.KeyValueDBSession{
			ID:            generateSessionID(),
			Authenticated: false,
			SelectedDB:    "",
			LastActive:    time.Now(),
			Conn:          conn,
			Mutex:         sync.Mutex{},
		},
		SubscribedChannels: []string{},
		Subscribers:        []protocol.Subscriber{},
	}

	log.Printf("New connection established: %s", session.ID)

	for {
		header := make([]byte, 9)
		_, err := io.ReadFull(reader, header)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading header from session %s: %v", session.ID, err)
			}
			break
		}

		commandID := binary.BigEndian.Uint32(header[0:4])
		commandType := protocol.CommandType(header[4]) // Cast to protocol.CommandType
		payloadSize := binary.BigEndian.Uint32(header[5:9])

		payload := make([]byte, payloadSize)
		_, err = io.ReadFull(reader, payload)
		if err != nil {
			log.Printf("Error reading payload from session %s: %v", session.ID, err)
			sendResponse(session, commandID, uint32(protocol.StatusError), "Error reading payload")
			continue
		}

		session.LastActive = time.Now()
		processCommand(session, commandID, commandType, payload)
	}

	log.Printf("Connection closed: %s", session.ID)
}

func processCommand(session *Session, commandID uint32, commandType protocol.CommandType, payload []byte) {
	var status protocol.StatusCode = protocol.StatusSuccess
	var responseData string

	switch commandType {
	case protocol.CommandAuth:
		// Auth command expects: auth <username> <password> <database>
		parts := strings.Fields(string(payload))
		if len(parts) != 3 {
			status = protocol.StatusError
			responseData = "Invalid auth command format. Use: auth <username> <password> <database>"
			break
		}
		username, password, database := parts[0], parts[1], parts[2]
		if authenticateUser(username, password, database) {
			session.Authenticated = true
			session.SelectedDB = database
			responseData = "Authentication successful"
		} else {
			status = protocol.StatusError
			responseData = "Authentication failed"
		}
	case protocol.CommandInsert:
		if !session.Authenticated || session.SelectedDB == "" {
			status = protocol.StatusError
			responseData = "Please authenticate and select a database first"
			break
		}
		// Insert command expects: insert <key>=<value>
		parts := strings.SplitN(string(payload), "=", 2)
		if len(parts) != 2 {
			status = protocol.StatusError
			responseData = "Invalid insert command format. Use: insert <key>=<value>"
			break
		}
		key, value := parts[0], parts[1]
		if err := core.InsertKey(session.SelectedDB, key, value); err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Insert failed: %v", err)
			break
		}
		responseData = "Insert successful"
	case protocol.CommandUpdate:
		if !session.Authenticated || session.SelectedDB == "" {
			status = protocol.StatusError
			responseData = "Please authenticate and select a database first"
			break
		}
		// Update command expects: update <key>=<newvalue>
		parts := strings.SplitN(string(payload), "=", 2)
		if len(parts) != 2 {
			status = protocol.StatusError
			responseData = "Invalid update command format. Use: update <key>=<newvalue>"
			break
		}
		key, value := parts[0], parts[1]
		if err := core.UpdateKey(session.SelectedDB, key, value); err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Update failed: %v", err)
			break
		}
		responseData = "Update successful"
	case protocol.CommandDelete:
		if !session.Authenticated || session.SelectedDB == "" {
			status = protocol.StatusError
			responseData = "Please authenticate and select a database first"
			break
		}
		// Delete command expects: delete <key>
		key := string(payload)
		if err := core.DeleteKey(session.SelectedDB, key); err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Delete failed: %v", err)
			break
		}
		responseData = "Delete successful"
	case protocol.CommandRead:
		if !session.Authenticated || session.SelectedDB == "" {
			status = protocol.StatusError
			responseData = "Please authenticate and select a database first"
			break
		}
		// Read command expects: read <key>
		key := string(payload)
		value, err := core.ReadKey(session.SelectedDB, key)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Read failed: %v", err)
		} else if value == "" {
			status = protocol.StatusError
			responseData = "Key not found"
		} else {
			responseData = value
		}
	case protocol.CommandBeginTx:
		if !session.Authenticated {
			status = protocol.StatusError
			responseData = "Please authenticate first"
			break
		}
		core.BeginTransaction(session.ID) // Assuming session.ID maps to clientID
		status = protocol.StatusTxBegin
		responseData = "Transaction started"
	case protocol.CommandCommitTx:
		if !session.Authenticated {
			status = protocol.StatusError
			responseData = "Please authenticate first"
			break
		}
		err := core.CommitTransaction(session.ID)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Commit failed: %v", err)
		} else {
			status = protocol.StatusTxCommit
			responseData = "Transaction committed"
		}
	case protocol.CommandRollbackTx:
		if !session.Authenticated {
			status = protocol.StatusError
			responseData = "Please authenticate first"
			break
		}
		err := core.RollbackTransaction(session.ID)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Rollback failed: %v", err)
		} else {
			status = protocol.StatusTxRollback
			responseData = "Transaction rolled back"
		}
	case protocol.CommandSetCache:
		// Payload format: set_cache <key>=<value>
		parts := strings.SplitN(string(payload), "=", 2)
		if len(parts) != 2 {
			status = protocol.StatusError
			responseData = "Invalid set_cache format. Use: set_cache <key>=<value>"
			break
		}
		key, value := parts[0], parts[1]
		core.SetCache(key, []byte(value))
		responseData = "Cache set successfully"
	case protocol.CommandGetCache:
		// Payload format: get_cache <key>
		key := string(payload)
		node, err := core.GetCache(key)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Get cache failed: %v", err)
			break
		}
		// Assuming node contains value as string
		if len(node.Keys) > 0 {
			responseData = fmt.Sprintf("Cache value: %s", string(node.Keys[0].Value))
		} else {
			responseData = "No value found in cache"
		}
	case protocol.CommandDeleteCache:
		// Payload format: delete_cache <key>
		key := string(payload)
		if err := core.DeleteCache(key); err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Delete cache failed: %v", err)
			break
		}
		responseData = "Cache entry deleted successfully"
	case protocol.CommandFlushCache:
		core.FlushCache()
		responseData = "Cache flushed successfully"
	case protocol.CommandPublish:
		// Payload format: publish <channel> <message>
		parts := strings.SplitN(string(payload), " ", 2)
		if len(parts) != 2 {
			status = protocol.StatusError
			responseData = "Invalid publish format. Use: publish <channel> <message>"
			break
		}
		channel, message := parts[0], parts[1]
		core.PublishMessage(channel, message)
		responseData = "Message published successfully"
	case protocol.CommandSubscribe:
		// Payload format: subscribe <channel>
		channel := string(payload)
		sub := core.SubscribeChannel(channel)
		session.SubscribedChannels = append(session.SubscribedChannels, channel)
		session.Subscribers = append(session.Subscribers, sub)

		// Start a goroutine to listen for messages and send to the client
		go func(sub protocol.Subscriber) {
			for msg := range sub {
				sendResponse(session, commandID, uint32(protocol.StatusSuccess), msg)
			}
		}(sub)
		responseData = fmt.Sprintf("Subscribed to channel '%s'", channel)
	case protocol.CommandDisconnect:
		// Handle client disconnection
		if session.Authenticated {
			core.DisconnectUser(session.ID)
		}
		responseData = "Disconnected successfully"
		connClose(session)
	case protocol.CommandListPush:
		// Payload format: <LPUSH|RPUSH> <key> <value>
		parts := strings.Fields(string(payload))
		if len(parts) != 3 {
			status = protocol.StatusError
			responseData = "Invalid list_push format. Use: list_push <LPUSH|RPUSH> <key> <value>"
			break
		}
		pushType, key, value := parts[0], parts[1], parts[2]
		if pushType == "LPUSH" {
			core.LPush(key, value)
		} else if pushType == "RPUSH" {
			core.RPush(key, value)
		} else {
			status = protocol.StatusError
			responseData = "Unknown push type. Use LPUSH or RPUSH"
			break
		}
		responseData = "List push successful"
	case protocol.CommandListRange:
		// Payload format: <key> <start> <stop>
		parts := strings.Fields(string(payload))
		if len(parts) != 3 {
			status = protocol.StatusError
			responseData = "Invalid list_range format. Use: list_range <key> <start> <stop>"
			break
		}
		key, startStr, stopStr := parts[0], parts[1], parts[2]
		start, err1 := strconv.Atoi(startStr)
		stop, err2 := strconv.Atoi(stopStr)
		if err1 != nil || err2 != nil {
			status = protocol.StatusError
			responseData = "Start and Stop must be integers"
			break
		}
		values, err := core.LRange(key, start, stop)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("List range failed: %v", err)
			break
		}
		responseData = strings.Join(values, ", ")
	case protocol.CommandSetAdd:
		// Payload format: set_add <key> <member>
		parts := strings.Fields(string(payload))
		if len(parts) != 2 {
			status = protocol.StatusError
			responseData = "Invalid set_add format. Use: set_add <key> <member>"
			break
		}
		key, member := parts[0], parts[1]
		core.SAdd(key, member)
		responseData = "Set add successful"
	case protocol.CommandSetMembers:
		// Payload format: set_members <key>
		key := string(payload)
		members, err := core.SMembers(key)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Set members failed: %v", err)
			break
		}
		responseData = strings.Join(members, ", ")
	case protocol.CommandHashSet:
		// Payload format: hash_set <key> <field> <value>
		parts := strings.Fields(string(payload))
		if len(parts) != 3 {
			status = protocol.StatusError
			responseData = "Invalid hash_set format. Use: hash_set <key> <field> <value>"
			break
		}
		key, field, value := parts[0], parts[1], parts[2]
		core.HSet(key, field, value)
		responseData = "Hash set successful"
	case protocol.CommandHashGet:
		// Payload format: hash_get <key> <field>
		parts := strings.Fields(string(payload))
		if len(parts) != 2 {
			status = protocol.StatusError
			responseData = "Invalid hash_get format. Use: hash_get <key> <field>"
			break
		}
		key, field := parts[0], parts[1]
		value, err := core.HGet(key, field)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("Hash get failed: %v", err)
			break
		}
		responseData = value
	case protocol.CommandZSetAdd:
		// Payload format: zset_add <key> <member> <score>
		parts := strings.Fields(string(payload))
		if len(parts) != 3 {
			status = protocol.StatusError
			responseData = "Invalid zset_add format. Use: zset_add <key> <member> <score>"
			break
		}
		key, member, scoreStr := parts[0], parts[1], parts[2]
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			status = protocol.StatusError
			responseData = "Score must be a float"
			break
		}
		core.ZAdd(key, member, score)
		responseData = "ZSet add successful"
	case protocol.CommandZSetRange:
		// Payload format: zset_range <key> <start> <stop>
		parts := strings.Fields(string(payload))
		if len(parts) != 3 {
			status = protocol.StatusError
			responseData = "Invalid zset_range format. Use: zset_range <key> <start> <stop>"
			break
		}
		key, startStr, stopStr := parts[0], parts[1], parts[2]
		start, err1 := strconv.Atoi(startStr)
		stop, err2 := strconv.Atoi(stopStr)
		if err1 != nil || err2 != nil {
			status = protocol.StatusError
			responseData = "Start and Stop must be integers"
			break
		}
		members, err := core.ZRange(key, start, stop)
		if err != nil {
			status = protocol.StatusError
			responseData = fmt.Sprintf("ZSet range failed: %v", err)
			break
		}
		responseData = strings.Join(members, ", ")
	default:
		status = protocol.StatusError
		responseData = "Unknown command type"
	}

	sendResponse(session, commandID, uint32(status), responseData)
}

func handleCustomCommand(session *Session, commandID uint32, payload []byte) {
	// This function can be expanded for additional custom commands if needed
}

// sendResponse sends a response back to the client.
func sendResponse(session *Session, commandID uint32, status uint32, data string) {
	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.BigEndian, commandID)
	binary.Write(&buffer, binary.BigEndian, status)
	dataBytes := []byte(data)
	dataSize := uint32(len(dataBytes))
	binary.Write(&buffer, binary.BigEndian, dataSize)
	buffer.Write(dataBytes)

	_, err := session.Conn.Write(buffer.Bytes())
	if err != nil {
		log.Printf("Error sending response to session %s: %v", session.ID, err)
	}
}

// authenticateUser authenticates a user using the core's AuthManager.
func authenticateUser(username, password, database string) bool {
	err := core.AuthenticateUser(username, password)
	if err != nil {
		log.Printf("Authentication failed for user %s: %v", username, err)
		return false
	}
	// Optionally, verify if the user has access to the specified database
	// For simplicity, assuming all authenticated users can access any database
	return true
}

// connClose handles client disconnection and cleanup.
func connClose(session *Session) {
	// Unsubscribe from all channels
	for i, channel := range session.SubscribedChannels {
		if i < len(session.Subscribers) {
			core.UnsubscribeChannel(channel, session.Subscribers[i])
		}
	}
	// Disconnect user if authenticated
	if session.Authenticated {
		core.DisconnectUser(session.ID)
	}
}

// generateSessionID generates a unique session ID.
func generateSessionID() string {
	return fmt.Sprintf("session-%d", time.Now().UnixNano())
}

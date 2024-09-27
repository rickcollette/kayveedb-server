package handler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rickcollette/kayveedb-server/core"
	"github.com/rickcollette/kayveedb-server/models"
	"github.com/rickcollette/kayveedb/protocol"
)

// HandleInsert processes the Insert command.
func HandleInsert(session *models.Session, key, value string) (protocol.StatusCode, string) {
	if session.SelectedDB == "" {
		return protocol.StatusError, "No database selected"
	}
	err := core.InsertKey(session.SelectedDB, key, value)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Insert failed: %v", err)
	}
	return protocol.StatusSuccess, "Insert successful"
}

// HandleUpdate processes the Update command.
func HandleUpdate(session *models.Session, key, newValue string) (protocol.StatusCode, string) {
	if session.SelectedDB == "" {
		return protocol.StatusError, "No database selected"
	}
	err := core.UpdateKey(session.SelectedDB, key, newValue)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Update failed: %v", err)
	}
	return protocol.StatusSuccess, "Update successful"
}

// HandleDelete processes the Delete command.
func HandleDelete(session *models.Session, key string) (protocol.StatusCode, string) {
	if session.SelectedDB == "" {
		return protocol.StatusError, "No database selected"
	}
	err := core.DeleteKey(session.SelectedDB, key)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Delete failed: %v", err)
	}
	return protocol.StatusSuccess, "Delete successful"
}

// HandleRead processes the Read command.
func HandleRead(session *models.Session, key string) (protocol.StatusCode, string) {
	if session.SelectedDB == "" {
		return protocol.StatusError, "No database selected"
	}
	value, err := core.ReadKey(session.SelectedDB, key)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Read failed: %v", err)
	}
	if value == "" {
		return protocol.StatusError, "Key not found"
	}
	return protocol.StatusSuccess, value
}

// HandleBeginTransaction processes the Begin Transaction command.
func HandleBeginTransaction(session *models.Session) (protocol.StatusCode, string) {
	core.BeginTransaction(session.ID)
	return protocol.StatusTxBegin, "Transaction started"
}

// HandleCommitTransaction processes the Commit Transaction command.
func HandleCommitTransaction(session *models.Session) (protocol.StatusCode, string) {
	err := core.CommitTransaction(session.ID)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Commit failed: %v", err)
	}
	return protocol.StatusTxCommit, "Transaction committed"
}

// HandleRollbackTransaction processes the Rollback Transaction command.
func HandleRollbackTransaction(session *models.Session) (protocol.StatusCode, string) {
	err := core.RollbackTransaction(session.ID)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Rollback failed: %v", err)
	}
	return protocol.StatusTxRollback, "Transaction rolled back"
}

// HandleSetCache processes the Set Cache command.
func HandleSetCache(session *models.Session, key string, value []byte) (protocol.StatusCode, string) {
	core.SetCache(key, value)
	return protocol.StatusSuccess, "Cache set successfully"
}

// HandleGetCache processes the Get Cache command.
func HandleGetCache(session *models.Session, key string) (protocol.StatusCode, string) {
	node, err := core.GetCache(key)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Get cache failed: %v", err)
	}
	if len(node.Keys) > 0 {
		return protocol.StatusSuccess, fmt.Sprintf("Cache value: %s", string(node.Keys[0].Value))
	}
	return protocol.StatusError, "No value found in cache"
}

// HandleDeleteCache processes the Delete Cache command.
func HandleDeleteCache(session *models.Session, key string) (protocol.StatusCode, string) {
	err := core.DeleteCache(key)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Delete cache failed: %v", err)
	}
	return protocol.StatusSuccess, "Cache entry deleted successfully"
}

// HandleFlushCache processes the Flush Cache command.
func HandleFlushCache(session *models.Session) (protocol.StatusCode, string) {
	core.FlushCache()
	return protocol.StatusSuccess, "Cache flushed successfully"
}

// HandlePublish processes the Publish command.
func HandlePublish(session *models.Session, channel, message string) (protocol.StatusCode, string) {
	core.PublishMessage(channel, message)
	return protocol.StatusSuccess, "Message published successfully"
}

// HandleSubscribe processes the Subscribe command.
func HandleSubscribe(session *models.Session, channel string) (protocol.StatusCode, string) {
	sub := core.SubscribeChannel(channel)
	session.SubscribedChannels = append(session.SubscribedChannels, channel)
	session.Subscribers = append(session.Subscribers, sub)

	// Start a goroutine to listen for messages and send to the client
	go func(sub protocol.Subscriber) {
		for msg := range sub {
			// Assuming a sendResponse function is available
			sendResponse(session, 0, uint32(protocol.StatusSuccess), msg)
		}
	}(sub)
	return protocol.StatusSuccess, fmt.Sprintf("Subscribed to channel '%s'", channel)
}

// HandleUnsubscribe processes the Unsubscribe command.
func HandleUnsubscribe(session *models.Session, channel string) (protocol.StatusCode, string) {
	for i, ch := range session.SubscribedChannels {
		if ch == channel {
			core.UnsubscribeChannel(channel, session.Subscribers[i])
			// Remove from slices
			session.SubscribedChannels = append(session.SubscribedChannels[:i], session.SubscribedChannels[i+1:]...)
			session.Subscribers = append(session.Subscribers[:i], session.Subscribers[i+1:]...)
			return protocol.StatusSuccess, fmt.Sprintf("Unsubscribed from channel '%s'", channel)
		}
	}
	return protocol.StatusError, fmt.Sprintf("Not subscribed to channel '%s'", channel)
}

// HandleListPush processes the List Push command.
func HandleListPush(session *models.Session, pushType, key, value string) (protocol.StatusCode, string) {
	if pushType == "LPUSH" {
		core.LPush(key, value)
	} else if pushType == "RPUSH" {
		core.RPush(key, value)
	} else {
		return protocol.StatusError, "Unknown push type. Use LPUSH or RPUSH"
	}
	return protocol.StatusSuccess, "List push successful"
}

// HandleListRange processes the List Range command.
func HandleListRange(session *models.Session, key string, start, stop int) (protocol.StatusCode, string) {
	values, err := core.LRange(key, start, stop)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("List range failed: %v", err)
	}
	return protocol.StatusSuccess, strings.Join(values, ", ")
}

// HandleSetAdd processes the Set Add command.
func HandleSetAdd(session *models.Session, key, member string) (protocol.StatusCode, string) {
	core.SAdd(key, member)
	return protocol.StatusSuccess, "Set add successful"
}

// HandleSetMembers processes the Set Members command.
func HandleSetMembers(session *models.Session, key string) (protocol.StatusCode, string) {
	members, err := core.SMembers(key)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Set members failed: %v", err)
	}
	return protocol.StatusSuccess, strings.Join(members, ", ")
}

// HandleHashSet processes the Hash Set command.
func HandleHashSet(session *models.Session, key, field, value string) (protocol.StatusCode, string) {
	core.HSet(key, field, value)
	return protocol.StatusSuccess, "Hash set successful"
}

// HandleHashGet processes the Hash Get command.
func HandleHashGet(session *models.Session, key, field string) (protocol.StatusCode, string) {
	value, err := core.HGet(key, field)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("Hash get failed: %v", err)
	}
	return protocol.StatusSuccess, value
}

// HandleZSetAdd processes the ZSet Add command.
func HandleZSetAdd(session *models.Session, key, member string, score float64) (protocol.StatusCode, string) {
	core.ZAdd(key, member, score)
	return protocol.StatusSuccess, "ZSet add successful"
}

// HandleZSetRange processes the ZSet Range command.
func HandleZSetRange(session *models.Session, key string, start, stop int) (protocol.StatusCode, string) {
	members, err := core.ZRange(key, start, stop)
	if err != nil {
		return protocol.StatusError, fmt.Sprintf("ZSet range failed: %v", err)
	}
	return protocol.StatusSuccess, strings.Join(members, ", ")
}

// sendResponse sends a response back to the client.
// This is a placeholder. Implement the actual response sending logic as needed.
func sendResponse(session *models.Session, commandID uint32, status uint32, data string) {
	// Implement the response sending based on your protocol.
	// This might involve serializing the response and writing to the connection.
}

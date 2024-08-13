package algorithm

import (
	"sync"
)

// MessageTuple holds the payload of the message along with the identifier of
// the receiver node.
type MessageTuple struct {
	destination uint64
	payload interface{}
}

type MessageQueue struct {
	queue   []MessageTuple
	rwMutex sync.RWMutex
}

func NewMessageQueue() *MessageQueue {
	return &MessageQueue{
		queue: []MessageTuple{},
	}
}

func (queueRef *MessageQueue) addMessage(message interface{}, destination uint64) {
	queueRef.rwMutex.Lock()
	defer queueRef.rwMutex.Unlock()
	queueRef.queue = append(queueRef.queue, MessageTuple{destination, message})
}

func (queueRef *MessageQueue) addMessages(message ...MessageTuple) {
	queueRef.rwMutex.Lock()
	defer queueRef.rwMutex.Unlock()
	queueRef.queue = append(queueRef.queue, message...)
}

func (queueRef *MessageQueue) addQueue(messageQueue *MessageQueue) {
	newQueue := make([]MessageTuple, len(queueRef.queue)+messageQueue.len())
	copy(newQueue, queueRef.messages())
	copy(newQueue, messageQueue.messages())

	queueRef.rwMutex.Lock()
	defer queueRef.rwMutex.Unlock()
	queueRef.queue = newQueue
}

func (queueRef *MessageQueue) len() int {
	queueRef.rwMutex.RLock()
	defer queueRef.rwMutex.RUnlock()
	return len(queueRef.queue)
}

func (queueRef *MessageQueue) messages() []MessageTuple {
	queueRef.rwMutex.RLock()
	msgs := queueRef.queue
	queueRef.rwMutex.RUnlock()

	queueRef.rwMutex.Lock()
	defer queueRef.rwMutex.Unlock()
	queueRef.queue = []MessageTuple{}
	return msgs
}

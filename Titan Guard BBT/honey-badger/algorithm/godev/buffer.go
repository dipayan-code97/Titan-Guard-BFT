package hbbft

import (
	"math/rand"
	"sync"
	"time"
)


// Buffer holds an uncommitted pool of arbitrary data. In blockchain parlance,
// this would be the unbound transaction list.
type Buffer struct {
	rwLock sync.RWMutex
	data   []*Transaction
}

// NewBuffer returns a new initialized buffer with the max data cap set to 1024.
func NewBuffer() *Buffer {
	return &Buffer{
		data: make([]*Transaction, 0, 1024),  // Updated to handle pointers to Transaction
	}
}

// push adds a transaction to the buffer.
func (bufferRef *Buffer) push(transaction *Transaction) {
	bufferRef.rwLock.Lock()
	defer bufferRef.rwLock.Unlock()
	bufferRef.data = append(bufferRef.data, transaction)
}

// delete removes the given slice of Transactions from the buffer.
func (bufferRef *Buffer) delete(transactions []*Transaction) {
	bufferRef.rwLock.Lock()
	defer bufferRef.rwLock.Unlock()

	// Use a map for efficient removal
	hashMap := make(map[string]bool)
	for _, text := range transactions {
		hashMap[string(text.hashcode())] = true
	}

	// Filter the buffer's data
	newData := bufferRef.data[:0]  // Reuse the underlying array
	for _, text := range bufferRef.data {
		if !hashMap[string(text.hashcode())] {
			newData = append(newData, text)
		}
	}
	bufferRef.data = newData
}

// len returns the current length of the buffer.
func (bufferRef *Buffer) len() int {
	bufferRef.rwLock.RLock()
	defer bufferRef.rwLock.RUnlock()
	return len(bufferRef.data)
}

// fetchSampleUpperBoundElements returns (n) elements from the buffer within an upper bound (m).
func (bufferRef *Buffer) fetchSampleUpperBoundElements(n, m int) []*Transaction {
	bufferRef.rwLock.RLock()
	defer bufferRef.rwLock.RUnlock()

	if bufferRef.len() <= 1 {
		return nil
	}

	if bufferRef.len() < m {
		m = bufferRef.len()
	}

	transactionSamples := make([]*Transaction, 0, n)
	for i := 0; i < n; i++ {
		index := rand.Intn(m)
		transactionSamples = append(transactionSamples, bufferRef.data[index])
	}
	return transactionSamples
}

// CreateSample returns a random sample of (n) transactions from the given slice.
func CreateSample(transactions []*Transaction, n int) []*Transaction {
	sampleTransactions := make([]*Transaction, 0, n)
	for index := 0; index < n; index++ {
		sampleTransactions = append(sampleTransactions, transactions[rand.Intn(len(transactions))])
	}
	return sampleTransactions
}

// Init seeds the random number generator.
func Init() {
	rand.Seed(time.Now().UnixNano())
}
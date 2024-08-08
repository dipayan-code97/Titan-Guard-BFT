package hbbft

import (
	"bytes"
	"encoding/gob"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// HBMessage is the top level message. It holds the epoch where the message was
// created and the actual payload.
type HBMessage struct {
	Epoch   uint64
	Payload interface{}
}

// Config holds the configuration of the top level HoneyBadger protocol as for
// its sub-protocols.
type Config struct {
	// Number of participating nodes.
	N int
	// Number of faulty nodes.
	F int
	// Unique identifier of the node.
	ID uint64
	// Identifiers of the participating nodes.
	Nodes []uint64
	// Maximum number of transactions that will be comitted in one epoch.
	BatchSize int
}

// HoneyBadger represents the top-level protocol of the hbbft consensus.
type HoneyBadger struct {
	// Config holds the configuration of the engine. This may not change
	// after engine initialization.
	Config
	// The instances of the "Common Subset Algorithm for some epoch.
	acsInstances map[uint64]*ACS
	// The unbound buffer of transactions.
	txBuffer *Buffer
	// current epoch.
	epoch uint64

	lock sync.RWMutex
	// Transactions that are commited in the corresponding epochs.
	outputs map[uint64][]Transaction
	// Que of messages that need to be broadcast after processing a message.
	messageQue *MessageQueue
	// Counter that counts the number of messages sent in one epoch.
	msgCount int
}

// NewHoneyBadger returns a new HoneyBadger instance.
func NewHoneyBadger(config Config) *HoneyBadger {
	return &HoneyBadger{
		Config:       config,
		acsInstances: make(map[uint64]*ACS),
		txBuffer:     NewBuffer(),
		outputs:      make(map[uint64][]Transaction),
		messageQue:   NewMessageQueue(),
	}
}

// Messages returns all the internal messages from the message que. Note that
// the que will be empty after invoking this method.
func (honeyBadgerRef *HoneyBadger) getMessages() []MessageTuple {
	return honeyBadgerRef.messageQue.messages()
}

// AddTransaction adds the given transaction to the internal buffer.
func (honeyBadgerRef *HoneyBadger) addTransaction(tx Transaction) {
	honeyBadgerRef.txBuffer.push(tx)
}

// HandleMessage will process the given ACSMessage for the given epoch.
func (honeyBadgerRef *HoneyBadger) handleMessage(sid, epoch uint64, msg *ACSMessage) error {
	honeyBadgerRef.msgCount++
	acs, ok := honeyBadgerRef.acsInstances[epoch]
	if !ok {
		// Ignore this message, it comes from an older epoch.
		if epoch < honeyBadgerRef.epoch {
			log.Warnf("ignoring old epoch")
			return nil
		}
		acs = BuildNewACS(honeyBadgerRef.Config)
		honeyBadgerRef.acsInstances[epoch] = acs
	}
	if err := acs.handleMessage(sid, msg); err != nil {
		return err
	}
	honeyBadgerRef.addMessages(acs.messageQueue.messages())
	if honeyBadgerRef.epoch == epoch {
		return honeyBadgerRef.maybeProcessOutput()
	}
	honeyBadgerRef.removeOldEpochs(epoch)
	return nil
}

// Start attempt to start the consensus engine.
// TODO(@anthdm): Reconsider API change.
func (honeyBadgerRef *HoneyBadger) start() error {
	return honeyBadgerRef.propose()
}

// LenMempool returns the number of transactions in the buffer.
func (honeyBadgerRef *HoneyBadger) lenMemoryPool() int {
	return honeyBadgerRef.txBuffer.len()
}

// Outputs returns the commited transactions per epoch.
func (honeyBadgerRef *HoneyBadger) processOutputs() map[uint64][]Transaction {
	honeyBadgerRef.lock.RLock()
	out := honeyBadgerRef.outputs
	honeyBadgerRef.lock.RUnlock()

	honeyBadgerRef.lock.Lock()
	defer honeyBadgerRef.lock.Unlock()
	honeyBadgerRef.outputs = make(map[uint64][]Transaction)
	return out
}

// propose will propose a new batch for the current epoch.
func (honeyBadgerRef *HoneyBadger) propose() error {
	if honeyBadgerRef.txBuffer.len() == 0 {
		time.Sleep(2 * time.Second)
		return honeyBadgerRef.propose()
	}
	batchSize := honeyBadgerRef.BatchSize
	// If no batch size is configured, choose somewhat of an ideal batch size
	// that will scale with the number of nodes added to the network as
	// decribed in the paper.
	if batchSize == 0 {
		// TODO: clean this up, factor out into own function? Make batch size
		// configurable.
		scalar := 20
		batchSize = (len(honeyBadgerRef.Nodes) * 2) * scalar
	}
	batchSize = int(math.Min(float64(batchSize), float64(honeyBadgerRef.txBuffer.len())))
	n := int(math.Max(float64(1), float64(batchSize/len(honeyBadgerRef.Nodes))))
	batch := CreateSample(honeyBadgerRef.txBuffer.data[:batchSize], n)

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(batch); err != nil {
		return err
	}
	acs := honeyBadgerRef.getOrNewACSInstance(honeyBadgerRef.epoch)
	if err := acs.setInputValue(buf.Bytes()); err != nil {
		return err
	}
	honeyBadgerRef.addMessages(acs.messageQueue.messages())
	return nil
}

func (honeyBadgerRef *HoneyBadger) maybeProcessOutput() error {
	start := time.Now()
	acs, ok := honeyBadgerRef.acsInstances[honeyBadgerRef.epoch]
	if !ok {
		return nil
	}
	outputs := acs.getACSInstance()
	if outputs == nil || len(outputs) == 0 {
		return nil
	}

	txMap := make(map[string]Transaction)
	for _, output := range outputs {
		var txx []Transaction
		if err := gob.NewDecoder(bytes.NewReader(output)).Decode(&txx); err != nil {
			return err
		}
		// Dedup the transactions, buffers could occasionally pick the same
		// transaction.
		for _, tx := range txx {
			txMap[string(tx.Hash())] = tx
		}
	}
	txBatch := make([]Transaction, len(txMap))
	i := 0
	for _, tx := range txMap {
		txBatch[i] = tx
		i++
	}
	// Delete the transactions from the buffer.
	honeyBadgerRef.txBuffer.delete(txBatch)
	// Add the transaction to the commit log.
	honeyBadgerRef.outputs[honeyBadgerRef.epoch] = txBatch
	honeyBadgerRef.epoch++

	if honeyBadgerRef.epoch%100 == 0 {
		log.Debugf("node (%d) commited (%d) transactions in epoch (%d) took %v",
			honeyBadgerRef.ID, len(txBatch), honeyBadgerRef.epoch, time.Since(start))
	}
	honeyBadgerRef.msgCount = 0

	return honeyBadgerRef.propose()
}

func (hb *HoneyBadger) getOrNewACSInstance(epoch uint64) *ACS {
	if acs, ok := hb.acsInstances[epoch]; ok {
		return acs
	}
	acs := BuildNewACS(hb.Config)
	hb.acsInstances[epoch] = acs
	return acs
}

// removeOldEpochs removes the ACS instances that have already been terminated.
func (honeyBadgerRef *HoneyBadger) removeOldEpochs(epoch uint64) {
	for i, acs := range honeyBadgerRef.acsInstances {
		if i >= honeyBadgerRef.epoch-1 {
			continue
		}
		for _, t := range acs.bbaInstances {
			t.stop()
		}
		for _, t := range acs.rbcInstances {
			t.stop()
		}
		acs.stop()
		delete(honeyBadgerRef.acsInstances, i)
	}
}

func (honeyBadgerRef *HoneyBadger) addMessages(messageTuple []MessageTuple) {
	for _, msg := range messageTuple {
		honeyBadgerRef.messageQue.addMessage(HBMessage{honeyBadgerRef.epoch, msg.Payload}, msg.To)
	}
}

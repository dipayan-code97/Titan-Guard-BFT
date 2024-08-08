package hbbft

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

// AgreementMessage holds the epoch and the message sent in the BBA protocol.
type AgreementMessage struct {
	// Epoch when this message was sent.
	Epoch int
	// The actual contents of the message.
	Message interface{}
}

// NewAgreementMessage constructs a new AgreementMessage.
func NewAgreementMessage(epoch int, message interface{}) *AgreementMessage {
	return &AgreementMessage{
		Epoch:   epoch,
		Message: message,
	}
}

// Bval Request holds the input value of the binary input.
type BinaryValueRequest struct {
	binaryValue bool
}

// Auxillary Request holds the output value.
type AuxillaryRequest struct {
	outputValue bool
}

// Binary Byzantine Agreement is build from a common coin protocol.
type BinaryByzantineAgreement struct {
	// Config holds the BBA configuration.
	Config
	// Current epoch.
	epoch uint32
	//  Bval requests we accepted this epoch.
	binValues []bool
	// sentBvals are the binary values this instance sent.
	sentBvals []bool
	// receivedBinaryValue is a mapping of the sender and the received binary value.
	receivedBinaryValue map[uint64]bool
	// receivedAuxillary is a mapping of the sender and the received Aux value.
	receivedAuxillary map[uint64]bool
	// Whether this bba is terminated or not.
	done bool
	// output and estimated of the bba protocol. This can be either nil or a
	// boolean.
	output, estimated, decision interface{}
	//delayedMessages are messages that are received by a node that is already
	// in a later epoch. These messages will be qued an handled the next epoch.
	delayedMessages []delayedMessage

	rwLocker sync.RWMutex
	// Que of AgreementMessages that need to be broadcasted after each received
	// message.
	agreementMessages []*AgreementMessage

	// control flow tuples for internal channel communication.
	closeCh   chan struct{}
	inputCh   chan bbaInputTuple
	messageCh chan bbaMessageTuple
	messageCount  int
}

// NewBBA returns a new instance of the Binary Byzantine Agreement.
func NewBBA(config Config) *BinaryByzantineAgreement {
	if config.F == 0 {
		config.F = (config.N - 1) / 3
	}
	binaryByzantineAgreement := &BinaryByzantineAgreement{
		Config:              config,
		receivedBinaryValue: make(map[uint64]bool),
		receivedAuxillary:   make(map[uint64]bool),
		sentBvals:           []bool{},
		binValues:           []bool{},
		closeCh:             make(chan struct{}),
		inputCh:             make(chan bbaInputTuple),
		messageCh:           make(chan bbaMessageTuple),
		agreementMessages:            []*AgreementMessage{},
		delayedMessages:     []delayedMessage{},
	}
	go binaryByzantineAgreement.run()
	return binaryByzantineAgreement
}

// Control flow structure for internal channel communication. Allowing us to
// avoid the use of mutexes and eliminates race conditions.
type (
	bbaMessageTuple struct {
		senderID         uint64
		agreementMessage *AgreementMessage
		err              chan error
	}

	bbaInputTuple struct {
		value bool
		err   chan error
	}

	delayedMessage struct {
		sid              uint64
		agreementMessage *AgreementMessage
	}
)

// InputValue will set the given val as the initial value to be proposed in the
// Agreement and returns an initial AgreementMessage or an error.
func (agreementRef *BinaryByzantineAgreement) inputValue(value bool) error {
	bbaTuple := bbaInputTuple{
		value: value,
		err:   make(chan error),
	}
	agreementRef.inputCh <- bbaTuple
	return <-bbaTuple.err
}

// HandleMessage will process the given rpc message. The caller is resposible to
// make sure only RPC messages are passed that are elligible for the BBA protocol.
func (bbaRef *BinaryByzantineAgreement) handleMessage(senderID uint64, message *AgreementMessage) error {
	bbaRef.messageCount++
	bbaMessage := bbaMessageTuple{
		senderID:         senderID,
		agreementMessage: message,
		err:              make(chan error),
	}
	bbaRef.messageCh <- bbaMessage
	return <-bbaMessage.err
}

// AcceptInput returns true whether this bba instance is elligable for accepting
// a new input value.
func (bbaRef *BinaryByzantineAgreement) acceptInput() bool {
	return bbaRef.epoch == 0 && bbaRef.estimated == nil
}

// Output will return the output of the bba instance. If the output was not nil
// then it will return the output else nil. Note that after consuming the output
// its will be set to nil forever.
func (bbaRef *BinaryByzantineAgreement) outputBinaryByzantineAgreement() interface{} {
	if bbaRef.output != nil {
		output := bbaRef.output
		bbaRef.output = nil
		return output
	}
	return nil
}

// Messages returns the queue of messages. The message que get's filled after
// processing a protocol message. After calling this method the que will
// be empty. Hence calling Messages can only occur once in a single roundtrip.
func (bbaRef *BinaryByzantineAgreement) messages() []*AgreementMessage {
	bbaRef.rwLocker.RLock()
	bbaMessage := bbaRef.messages
	bbaRef.rwLocker.RUnlock()

	bbaRef.rwLocker.Lock()
	defer bbaRef.rwLocker.Unlock()
	bbaRef.agreementMessages = []*AgreementMessage{}
	return bbaMessage
}


// addMessage appends a new agreement message to the list of messages.
//
// Parameters:
// - agreementMessage: the agreement message to be added.
func (bbaRef *BinaryByzantineAgreement) addMessage(agreementMessage *AgreementMessage) {
	bbaRef.rwLocker.Lock()
	defer bbaRef.rwLocker.Unlock()

	// Append the new agreement message to the list of messages.
	bbaRef.agreementMessages = append(bbaRef.agreementMessages, agreementMessage)
}

func (bbaRef *BinaryByzantineAgreement) stop() {
	close(bbaRef.closeCh)
}

// run makes sure we only process 1 message at the same time, avoiding mutexes
// and race conditions.
func (bbaRef *BinaryByzantineAgreement) run() {
	for {
		select {
		case <-bbaRef.closeCh:
			return
		case t := <-bbaRef.inputCh:
			t.err <- bbaRef.inputValue(t.value)
		case t := <-bbaRef.messageCh:
			t.err <- bbaRef.handleMessage(t.senderID, t.agreementMessage)
		}
	}
}

// inputValue will set the given val as the initial value to be proposed in the
// Agreement.
func (bbaRef *BinaryByzantineAgreement) setInputValue(inputValue bool) error {
	// Make sure we are in the first epoch round.
	if bbaRef.epoch != 0 || bbaRef.estimated != nil {
		return nil
	}
	bbaRef.estimated = inputValue
	bbaRef.sentBvals = append(bbaRef.sentBvals, inputValue)
	bbaRef.addMessage(NewAgreementMessage(int(bbaRef.epoch), &BinaryValueRequest{inputValue}))
	return bbaRef.handleBinaryValueRequest(bbaRef.ID, inputValue)
}

// handleMessage will process the given rpc message. The caller is resposible to
// make sure only RPC messages are passed that are elligible for the BBA protocol.
func (bbaRef *BinaryByzantineAgreement) handleRPCMessage(senderID uint64, agreementMessage *AgreementMessage) error {
	if bbaRef.done {
		return nil
	}
	// Ignore messages from older epochs.
	if agreementMessage.Epoch < int(bbaRef.epoch) {
		log.Debugf(
			"id (%d) with epoch (%d) received msg from an older epoch (%d)",
			bbaRef.ID, bbaRef.epoch, agreementMessage.Epoch,
		)
		return nil
	}
	// Messages from later epochs will be qued and processed later.
	if agreementMessage.Epoch > int(bbaRef.epoch) {
		bbaRef.delayedMessages = append(bbaRef.delayedMessages, delayedMessage{senderID, agreementMessage})
		return nil
	}

	switch t := agreementMessage.Message.(type) {
	case *BinaryValueRequest:
		return bbaRef.handleBinaryValueRequest(senderID, t.binaryValue)
	case *AuxillaryRequest:
		return bbaRef.handleAuxillaryRequest(senderID, t.outputValue)
	default:
		return fmt.Errorf("unknown BBA message received: %v", t)
	}
}

// handleBvalRequest processes the received binary value and fills up the
// message que if there are any messages that need to be broadcasted.
func (agreementRef *BinaryByzantineAgreement) handleBinaryValueRequest(senderID uint64, binaryValue bool) error {
	agreementRef.rwLocker.Lock()
	agreementRef.receivedBinaryValue[senderID] = binaryValue
	agreementRef.rwLocker.Unlock()
	lenBval := agreementRef.countBvals(binaryValue)

	// When receiving n bval(b) messages from 2f+1 nodes: inputs := inputs u {b}
	if lenBval == 2*agreementRef.F+1 {
		wasEmptyBinValues := len(agreementRef.binValues) == 0
		agreementRef.binValues = append(agreementRef.binValues, binaryValue)
		// If inputs > 0 broadcast output(b) and handle the output ourselfs.
		// Wait until binValues > 0, then broadcast AUX(b). The AUX(b) broadcast
		// may only occure once per epoch.
		if wasEmptyBinValues {
			agreementRef.addMessage(NewAgreementMessage(int(agreementRef.epoch), &AuxillaryRequest{binaryValue}))
			agreementRef.handleAuxillaryRequest(agreementRef.ID, binaryValue)
		}
		return nil
	}
	// When receiving input(b) messages from f + 1 nodes, if inputs(b) is not
	// been sent yet broadcast input(b) and handle the input ourselfs.
	if lenBval == agreementRef.F+1 && !agreementRef.hasSentBinaryValue(binaryValue) {
		agreementRef.sentBvals = append(agreementRef.sentBvals, binaryValue)
		agreementRef.addMessage(NewAgreementMessage(int(agreementRef.epoch), &BinaryValueRequest{binaryValue}))
		return agreementRef.handleBinaryValueRequest(agreementRef.ID, binaryValue)
	}
	return nil
}

func (agreementRef *BinaryByzantineAgreement) handleAuxillaryRequest(senderID uint64, binaryValue bool) error {
	agreementRef.rwLocker.Lock()
	agreementRef.receivedAuxillary[senderID] = binaryValue
	agreementRef.rwLocker.Unlock()
	agreementRef.tryOutputAgreement()
	return nil
}

// tryOutputAgreement waits until at least (N - f) output messages received,
// once the (N - f) messages are received, make a common coin and uses it to
// compute the next decision estimate and output the optional decision value.
func (agreementRef *BinaryByzantineAgreement) tryOutputAgreement() {
	if len(agreementRef.binValues) == 0 {
		return
	}
	// Wait longer till eventually receive (N - F) aux messages.
	lenOutputs, values := agreementRef.countOutputs()
	if lenOutputs < agreementRef.N-agreementRef.F {
		return
	}

	// TODO: implement a real common coin algorithm.
	coin := agreementRef.epoch%2 == 0

	// Continue the BBA until both:
	// - a value b is output in some epoch r
	// - the value (coin r) = b for some round r' > r
	if agreementRef.done || agreementRef.decision != nil && agreementRef.decision.(bool) == coin {
		agreementRef.done = true
		return
	}

	log.Debugf(
		"id (%d) is advancing to the next epoch! (%d) received (%d) aux messages",
		agreementRef.ID, agreementRef.epoch+1, lenOutputs,
	)

	// Start the next epoch.
	agreementRef.advanceEpoch()

	if len(values) != 1 {
		agreementRef.estimated = coin
	} else {
		agreementRef.estimated = values[0]
		// getACSInstance may be set only once.
		if agreementRef.decision == nil && values[0] == coin {
			agreementRef.output = values[0]
			agreementRef.decision = values[0]
			log.Debugf("id (%d) outputed a decision (%v) after (%d) msgs", agreementRef.ID, values[0], agreementRef.messageCount)
			agreementRef.messageCount = 0
		}
	}
	estimated := agreementRef.estimated.(bool)
	agreementRef.sentBvals = append(agreementRef.sentBvals, estimated)
	agreementRef.addMessage(NewAgreementMessage(int(agreementRef.epoch), &BinaryValueRequest{estimated}))

	// handle the delayed messages.
	for _, queue := range agreementRef.delayedMessages {
		if err := agreementRef.handleMessage(queue.sid, queue.agreementMessage); err != nil {
			// TODO: Handle this error properly.
			log.Warn(err)
		}
	}
	agreementRef.delayedMessages = []delayedMessage{}
}

// advanceEpoch will reset all the values that are bound to an epoch and increments
// the epoch value by 1.
func (agreementRef *BinaryByzantineAgreement) advanceEpoch() {
	agreementRef.binValues = []bool{}
	agreementRef.sentBvals = []bool{}
	agreementRef.receivedAuxillary = make(map[uint64]bool)
	agreementRef.receivedBinaryValue = make(map[uint64]bool)
	agreementRef.epoch++
}

// countOutputs returns the number of received (aux) messages, the corresponding
// values that where also in our inputs.
func (agreementRef *BinaryByzantineAgreement) countOutputs() (int, []bool) {
	receivedAuxilaryMessages := map[bool]int{}
	for index, value := range agreementRef.receivedAuxillary {
		receivedAuxilaryMessages[value] = int(index)
	}
	valuesDeck := []bool{}
	for _, value := range agreementRef.binValues {
		if _, ok := receivedAuxilaryMessages[value]; ok {
			valuesDeck = append(valuesDeck, value)
		}
	}
	return len(agreementRef.receivedAuxillary), valuesDeck
}

// countBvals counts all the received Bval inputs matching b.
func (agreementRef *BinaryByzantineAgreement) countBvals(ok bool) int {
	agreementRef.rwLocker.RLock()
	defer agreementRef.rwLocker.RUnlock()
	n := 0
	for _, value := range agreementRef.receivedBinaryValue {
		if value == ok {
			n++
		}
	}
	return n
}

// hasSentBval return true if we already sent out the given value.
func (agreementRef *BinaryByzantineAgreement) hasSentBinaryValue(binaryValue bool) bool {
	for _, ok := range agreementRef.sentBvals {
		if ok == binaryValue {
			return true
		}
	}
	return false
}

package hbbft

import (
	"fmt"
)

// ACSMessage represents a message sent between nodes in the ACS protocol.
type ACSMessage struct {
	// Unique identifier of the "proposing" node.
	ProposerID uint64
	// Actual payload beeing sent.
	Payload interface{}
}

// ACS implements the Asynchronous Common Subset protocol.
// ACS assumes a network of N nodes that send signed messages to each other.
// There can be f faulty nodes where (3 * f < N).
// Each participating node proposes an element for inlcusion. The protocol
// guarantees that all the good nodes output the same set, consisting of
// at least (N -f) of the proposed values.
//
// Algorithm:
// ACS creates a Broadcast algorithm for each of the participating nodes.
// At least (N -f) of these will eventually output the element proposed by that
// node. ACS will also create and BBA instance for each participating node, to
// decide whether that node's proposed element should be inlcuded in common set.
// Whenever an element is received via broadcast, we imput "true" into the
// corresponding BBA instance. When (N-f) BBA instances have decided true we
// input false into the remaining ones, where we haven't provided input yet.
// Once all BBA instances have decided, ACS returns the set of all proposed
// values for which the decision was truthy.
type ACS struct {
	// Config holds the ACS configuration.
	Config
	// Mapping of node ids and their rbc instance.
	rbcInstances map[uint64]*RBC
	// Mapping of node ids and their bba instance.
	bbaInstances map[uint64]*BinaryByzantineAgreement
	// Results of the Reliable Broadcast.
	rbcResults map[uint64][]byte
	// Results of the Binary Byzantine Agreement.
	bbaResults map[uint64]bool
	// Final output of the ACS.
	output map[uint64][]byte
	// Que of ACSMessages that need to be broadcasted after each received
	// and processed a message.
	messageQueue *MessageQueue
	// Whether this ACS instance has already has decided output or not.
	decided bool

	// control flow tuples for internal channel communication.
	closeCh   chan struct{}
	inputCh   chan acsInputTuple
	messageCh chan acsMessageTuple
}

// Control flow structure for internal channel communication. Allowing us to
// avoid the use of mutexes and eliminates race conditions.
type (
	acsMessageTuple struct {
		senderID   uint64
		acsMessage *ACSMessage
		err        chan error
	}

	acsInputResponse struct {
		rbcMessages []*BroadcastMessage
		acsMessages []*ACSMessage
		err         error
	}

	acsInputTuple struct {
		value    []byte
		response chan acsInputResponse
	}
)

// BuildNewACS returns a new ACS instance configured with the given Config and node
// ids.
func BuildNewACS(config Config) *ACS {
	if config.F == 0 {
		config.F = (config.N - 1) / 3
	}
	acs := &ACS{
		Config:       config,
		rbcInstances: make(map[uint64]*RBC),
		bbaInstances: make(map[uint64]*BinaryByzantineAgreement),
		rbcResults:   make(map[uint64][]byte),
		bbaResults:   make(map[uint64]bool),
		messageQueue: NewMessageQueue(),
		closeCh:      make(chan struct{}),
		inputCh:      make(chan acsInputTuple),
		messageCh:    make(chan acsMessageTuple),
	}
	// Create all the instances for the participating nodes
	for _, id := range config.Nodes {
		acs.rbcInstances[id] = NewRBC(config, id)
		acs.bbaInstances[id] = NewBBA(config)
	}
	go acs.run()
	return acs
}

// setInputValue sets the input value for broadcast and returns an initial set of
// Broadcast and ACS Messages to be broadcasted in the network.
func (acsRef *ACS) setInputValue(value []byte) error {
	acsInputTuple := acsInputTuple{
		value:    value,
		response: make(chan acsInputResponse),
	}
	acsRef.inputCh <- acsInputTuple
	response := <-acsInputTuple.response
	return response.err
}

// handleMessage handles incoming messages to ACS and redirects them to the
// appropriate sub(protocol) instance.
func (acsRef *ACS) handleIncomingMessage(senderID uint64, acsMessage *ACSMessage) error {
	acsMessageTuple := acsMessageTuple{
		senderID:   senderID,
		acsMessage: acsMessage,
		err:        make(chan error),
	}
	acsRef.messageCh <- acsMessageTuple
	return <-acsMessageTuple.err
}

// handleMessage handles incoming messages to ACS and redirects them to the
// appropriate sub(protocol) instance.
func (acsRef *ACS) handleMessage(senderID uint64, acsMessage *ACSMessage) error {
	switch t := acsMessage.Payload.(type) {
	case *AgreementMessage:
		return acsRef.handleAgreement(senderID, acsMessage.ProposerID, t)
	case *BroadcastMessage:
		return acsRef.handleBroadcast(senderID, acsMessage.ProposerID, t)
	default:
		return fmt.Errorf("received unknown message (%v)", t)
	}
}

// getACSInstance will return the output of the ACS instance. If the output was not nil
// then it will return the output else nil. Note that after consuming the output
// its will be set to nil forever.
func (acsRef *ACS) getACSInstance() map[uint64][]byte {
	if acsRef.output != nil {
		out := acsRef.output
		acsRef.output = nil
		return out
	}
	return nil
}

// Done returns true whether ACS has completed its agreements and cleared its
// messageQue.
func (acsRef *ACS) done() bool {
	agreementsDone := true
	for _, bba := range acsRef.bbaInstances {
		if !bba.done {
			agreementsDone = false
		}
	}
	return agreementsDone && acsRef.messageQueue.len() == 0
}

// inputValue sets the input value for broadcast and returns an initial set of
// Broadcast and ACS Messages to be broadcasted in the network.
func (acsRef *ACS) inputValue(data []byte) error {
	rbc, ok := acsRef.rbcInstances[acsRef.ID]
	if !ok {
		return fmt.Errorf("could not find rbc instance (%d)", acsRef.ID)
	}
	requests, err := rbc.InputValue(data)
	if err != nil {
		return err
	}
	if len(requests) != acsRef.N-1 {
		return fmt.Errorf("expecting (%d) proof messages got (%d)", acsRef.N, len(requests))
	}
	for index, id := range UInt64sWithout(acsRef.Nodes, acsRef.ID) {
		acsRef.messageQueue.addMessage(&ACSMessage{acsRef.ID, requests[index]}, id)
	}
	for _, msg := range rbc.Messages() {
		acsRef.addMessage(acsRef.ID, msg)
	}
	if output := rbc.Output(); output != nil {
		acsRef.rbcResults[acsRef.ID] = output
		acsRef.processAgreement(acsRef.ID, func(bba *BinaryByzantineAgreement) error {
			if bba.AcceptInput() {
				return bba.InputValue(true)
			}
			return nil
		})
	}
	return nil
}

func (acsRef *ACS) stop() {
	close(acsRef.closeCh)
}

func (acsRef *ACS) run() {
	for {
		select {
		case <-acsRef.closeCh:
			return
		case t := <-acsRef.inputCh:
			err := acsRef.inputValue(t.value)
			t.response <- acsInputResponse{err: err}
		case t := <-acsRef.messageCh:
			t.err <- acsRef.handleMessage(t.senderID, t.acsMessage)
		}
	}
}

// handleAgreement processes the received AgreementMessage from sender (sid)
// for a value proposed by the proposing node (pid).
func (acsRef *ACS) handleAgreement(sid, pid uint64, agreementMessage *AgreementMessage) error {
	return acsRef.processAgreement(pid, func(bba *BinaryByzantineAgreement) error {
		return bba.HandleMessage(sid, agreementMessage)
	})
}

// handleBroadcast processes the received BroadcastMessage.
func (acsRef *ACS) handleBroadcast(sid, pid uint64, broadcastMessage *BroadcastMessage) error {
	return acsRef.processBroadcast(pid, func(rbc *RBC) error {
		return rbc.HandleMessage(sid, broadcastMessage)
	})
}

func (acsRef *ACS) processBroadcast(pid uint64, fun func(rbc *RBC) error) error {
	rbc, ok := acsRef.rbcInstances[pid]
	if !ok {
		return fmt.Errorf("could not find rbc instance for (%d)", pid)
	}
	if err := fun(rbc); err != nil {
		return err
	}
	for _, msg := range rbc.Messages() {
		acsRef.addMessage(pid, msg)
	}
	if output := rbc.Output(); output != nil {
		acsRef.rbcResults[pid] = output
		return acsRef.processAgreement(pid, func(bba *BinaryByzantineAgreement) error {
			if bba.AcceptInput() {
				return bba.InputValue(true)
			}
			return nil
		})
	}
	return nil
}

func (acsRef *ACS) processAgreement(pid uint64, fun func(bba *BinaryByzantineAgreement) error) error {
	bba, ok := acsRef.bbaInstances[pid]
	if !ok {
		return fmt.Errorf("could not find bba instance for (%d)", pid)
	}
	if bba.done {
		return nil
	}
	if err := fun(bba); err != nil {
		return err
	}
	for _, msg := range bba.Messages() {
		acsRef.addMessage(pid, msg)
	}
	// Check if we got an output.
	if output := bba.Output(); output != nil {
		if _, ok := acsRef.bbaResults[pid]; ok {
			return fmt.Errorf("multiple bba results for (%d)", pid)
		}
		acsRef.bbaResults[pid] = output.(bool)
		// When received 1 from at least (N - f) instances of BA, provide input 0.
		// to each other instance of BBA that has not provided his input yet.
		if output.(bool) && acsRef.countTruthyAgreements() == acsRef.N-acsRef.F {
			for id, bba := range acsRef.bbaInstances {
				if bba.AcceptInput() {
					if err := bba.InputValue(false); err != nil {
						return err
					}
					for _, msg := range bba.Messages() {
						acsRef.addMessage(id, msg)
					}
					if output := bba.Output(); output != nil {
						acsRef.bbaResults[id] = output.(bool)
					}
				}
			}
		}
		acsRef.tryCompleteAgreement()
	}
	return nil
}

func (acsRef *ACS) tryCompleteAgreement() {
	if acsRef.decided || acsRef.countTruthyAgreements() < acsRef.N-acsRef.F {
		return
	}
	if len(acsRef.bbaResults) < acsRef.N {
		return
	}
	// At this point all bba instances have provided their output.
	nodesThatProvidedTrue := []uint64{}
	for id, ok := range acsRef.bbaResults {
		if ok {
			nodesThatProvidedTrue = append(nodesThatProvidedTrue, id)
		}
	}
	bcResults := make(map[uint64][]byte)
	for _, id := range nodesThatProvidedTrue {
		val, _ := acsRef.rbcResults[id]
		bcResults[id] = val
	}
	if len(nodesThatProvidedTrue) == len(bcResults) {
		acsRef.output = bcResults
		acsRef.decided = true
	}
}

func (acsRef *ACS) addMessage(source uint64, message interface{}) {
	for _, id := range UInt64sWithout(acsRef.Nodes, acsRef.ID) {
		acsRef.messageQueue.addMessage(&ACSMessage{source, message}, id)
	}
}

// countTruthyAgreements returns the number of truthy received agreement messages.
func (acsRef *ACS) countTruthyAgreements() int {
	n := 0
	for _, ok := range acsRef.bbaResults {
		if ok {
			n++
		}
	}
	return n
}

func UInt64sWithout(source []uint64, value uint64) []uint64 {
	destination := []uint64{}
	for index := 0; index < len(source); index++ {
		if source[index] != value {
			destination = append(destination, source[index])
		}
	}
	return destination
}

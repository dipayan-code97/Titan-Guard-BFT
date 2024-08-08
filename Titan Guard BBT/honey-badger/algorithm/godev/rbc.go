package hbbft

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/NebulousLabs/merkletree"
	"github.com/klauspost/reedsolomon"
)

// BroadcastMessage holds the payload sent between nodes in the rbc protocol.
// Its basically just a wrapper to let top-level protocols distinguish incoming
// messages.
type BroadcastMessage struct {
	Payload interface{}
}

// ProofRequest holds the RootHash along with the Shard of the erasure encoded
// payload.
type ProofRequest struct {
	RootHash []byte
	// Proof[0] will containt the actual data.
	Proof         [][]byte
	Index, Leaves int
}

// EchoRequest represents the echoed version of the proof.
type EchoRequest struct {
	ProofRequest
}

// ReadyRequest holds the RootHash of the received proof and should be sent
// after receiving and validating enough proof chunks.
type ReadyRequest struct {
	RootHash []byte
}

type proofs []ProofRequest

func (proofRef proofs) Len() int           { return len(proofRef) }
func (proofRef proofs) Swap(i, j int)      { proofRef[i], proofRef[j] = proofRef[j], proofRef[i] }
func (proofRef proofs) Less(i, j int) bool { return proofRef[i].Index < proofRef[j].Index }

// RBC represents the instance of the "Reliable Broadcast Algorithm".
type RBC struct {
	// Config holds the configuration.
	Config
	// proposerID is the ID of the proposing node of this RB instance.
	proposerID uint64
	// The reedsolomon encoder to encode the proposed value into shards.
	encoder reedsolomon.Encoder
	// recvReadys is a mapping between the sender and the root hash that was
	// inluded in the ReadyRequest.
	recvReadys map[uint64][]byte
	// revcEchos is a mapping between the sender and the EchoRequest.
	recvEchos map[uint64]*EchoRequest
	// Number of the parity and data shards that will be used for erasure encoding
	// the given value.
	numParityShards, numDataShards int
	// Que of BroadcastMessages that need to be broadcasted after each received
	// and processed a message.
	messages []*BroadcastMessage
	// Booleans fields to determine operations on the internal state.
	echoSent, readySent, outputDecoded bool
	// The actual output this instance has produced.
	output []byte

	// control flow tuples for internal channel communication.
	closeCh   chan struct{}
	inputCh   chan rbcInputTuple
	messageCh chan rbcMessageTuple
}

type (
	rbcMessageTuple struct {
		senderID uint64
		broadcastMessage      *BroadcastMessage
		err      chan error
	}

	rbcInputResponse struct {
		messages []*BroadcastMessage
		err      error
	}

	rbcInputTuple struct {
		value    []byte
		response chan rbcInputResponse
	}
)

// NewRBC returns a new instance of the ReliableBroadcast configured
// with the given config
func NewRBC(config Config, proposerID uint64) *RBC {
	if config.F == 0 {
		config.F = (config.N - 1) / 3
	}
	var (
		parityShards = 2 * config.F
		dataShards   = config.N - parityShards
	)
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		panic(err)
	}
	rbc := &RBC{
		Config:          config,
		recvEchos:       make(map[uint64]*EchoRequest),
		recvReadys:      make(map[uint64][]byte),
		encoder:         enc,
		numParityShards: parityShards,
		numDataShards:   dataShards,
		messages:        []*BroadcastMessage{},
		proposerID:      proposerID,
		closeCh:         make(chan struct{}),
		inputCh:         make(chan rbcInputTuple),
		messageCh:       make(chan rbcMessageTuple),
	}
	go rbc.run()
	return rbc
}

// InputValue will set the given data as value V. The data will first splitted
// into shards and additional parity shards (used for reconstruction), the
// equally splitted shards will be fed into a reedsolomon encoder. After encoding,
// only the requests for the other participants are beeing returned.
func (rbcRef *RBC) inputValue(data []byte) ([]*BroadcastMessage, error) {
	rbc := rbcInputTuple{
		value:    data,
		response: make(chan rbcInputResponse),
	}
	rbcRef.inputCh <- rbc
	response := <-rbc.response
	return response.messages, response.err
}

// HandleMessage will process the given rpc message and will return a possible
// outcome. The caller is resposible to make sure only RPC messages are passed
// that are elligible for the RBC protocol.
func (rbcRef *RBC) handleMessage(senderID uint64, msg *BroadcastMessage) error {
	rbc := rbcMessageTuple{
		senderID: senderID,
		broadcastMessage:      msg,
		err:      make(chan error),
	}
	rbcRef.messageCh <- rbc
	return <-rbc.err
}

func (rbcRef *RBC) stop() {
	close(rbcRef.closeCh)
}

func (rbcRef *RBC) run() {
	for {
		select {
		case <-rbcRef.closeCh:
			return
		case t := <-rbcRef.inputCh:
			msgs, err := rbcRef.inputValue(t.value)
			t.response <- rbcInputResponse{
				messages: msgs,
				err:      err,
			}
		case t := <-rbcRef.messageCh:
			t.err <- rbcRef.handleMessage(t.senderID, t.broadcastMessage)
		}
	}
}

func (rbcRef *RBC) inputValue(data []byte) ([]*BroadcastMessage, error) {
	shards, err := MakeShards(rbcRef.encoder, data)
	if err != nil {
		return nil, err
	}
	reqs, err := MakeBroadcastMessages(shards)
	if err != nil {
		return nil, err
	}
	// The first request is for ourselfs. The rests is distributed under the
	// participants.
	proof := reqs[0].Payload.(*ProofRequest)
	if err := rbcRef.handleProofRequest(rbcRef.ID, proof); err != nil {
		return nil, err
	}
	return reqs[1:], nil
}

func (rbcRef *RBC) handleMessage(senderID uint64, msg *BroadcastMessage) error {
	switch t := msg.Payload.(type) {
	case *ProofRequest:
		return rbcRef.handleProofRequest(senderID, t)
	case *EchoRequest:
		return rbcRef.handleEchoRequest(senderID, t)
	case *ReadyRequest:
		return rbcRef.handleReadyRequest(senderID, t)
	default:
		return fmt.Errorf("invalid RBC protocol message: %+v", msg)
	}
}

// Messages returns the que of messages. The message que get's filled after
// processing a protocol message. After calling this method the que will
// be empty. Hence calling Messages can only occur once in a single roundtrip.
func (rbcRef *RBC) getMessages() []*BroadcastMessage {
	messagesDeck := rbcRef.messages
	rbcRef.messages = []*BroadcastMessage{}
	return messagesDeck
}

// Output will return the output of the rbc instance. If the output was not nil
// then it will return the output else nil. Note that after consuming the output
// its will be set to nil forever.
func (rbcRef *RBC) fetchOutput() []byte {
	if rbcRef.output != nil {
		out := rbcRef.output
		rbcRef.output = nil
		return out
	}
	return nil
}

// When a node receives a Proof from a proposer it broadcasts the proof as an
// EchoRequest to the network after validating its content.
func (rbcRef *RBC) handleProofRequest(senderID uint64, request *ProofRequest) error {
	if senderID != rbcRef.proposerID {
		return fmt.Errorf(
			"receiving proof from (%d) that is not from the proposing node (%d)",
			senderID, rbcRef.proposerID,
		)
	}
	if rbcRef.echoSent {
		return fmt.Errorf("received proof from (%d) more the once", senderID)
	}
	if !ValidateProof(request) {
		return fmt.Errorf("received invalid proof from (%d)", senderID)
	}
	rbcRef.echoSent = true
	echo := &EchoRequest{*request}
	rbcRef.messages = append(rbcRef.messages, &BroadcastMessage{echo})
	return rbcRef.handleEchoRequest(rbcRef.ID, echo)
}

// Every node that has received (N - f) echo's with the same root hash from
// distinct nodes knows that at least (f + 1) "good" nodes have sent an echo
// with that root hash to every participant. Upon receiving (N - f) echo's we
// broadcast a ReadyRequest with the roothash. Even without enough echos, if a
// node receives (f + 1) ReadyRequests we know that at least one good node has
// sent Ready, hence also knows that everyone will be able to decode eventually
// and broadcast ready itself.
func (rbcRef *RBC) handleEchoRequest(senderID uint64, request *EchoRequest) error {
	if _, ok := rbcRef.recvEchos[senderID]; ok {
		return fmt.Errorf(
			"received multiple echos from (%d) my id (%d)", senderID, rbcRef.ID)
	}
	if !ValidateProof(&request.ProofRequest) {
		return fmt.Errorf(
			"received invalid proof from (%d) my id (%d)", senderID, rbcRef.ID)
	}

	rbcRef.recvEchos[senderID] = request
	if rbcRef.readySent || rbcRef.countEchos(request.RootHash) < rbcRef.N-rbcRef.F {
		return rbcRef.tryDecodeValue(request.RootHash)
	}

	rbcRef.readySent = true
	ready := &ReadyRequest{request.RootHash}
	rbcRef.messages = append(rbcRef.messages, &BroadcastMessage{ready})
	return rbcRef.handleReadyRequest(rbcRef.ID, ready)
}

// If a node had received (2 * f + 1) ready's (with matching root hash)
// from distinct nodes, it knows that at least (f + 1) good nodes have sent
// it. Hence every good node will eventually receive (f + 1) and broadcast
// ready itself. Eventually a node with (2 * f + 1) readys and (f + 1) echos
// will decode and ouput the value, knowing that every other good node will
// do the same.
func (rbcRef *RBC) handleReadyRequest(senderID uint64, request *ReadyRequest) error {
	if _, ok := rbcRef.recvReadys[senderID]; ok {
		return fmt.Errorf("received multiple readys from (%d)", senderID)
	}
	rbcRef.recvReadys[senderID] = request.RootHash

	if rbcRef.countReadys(request.RootHash) == rbcRef.F+1 && !rbcRef.readySent {
		rbcRef.readySent = true
		ready := &ReadyRequest{request.RootHash}
		rbcRef.messages = append(rbcRef.messages, &BroadcastMessage{ready})
	}
	return rbcRef.tryDecodeValue(request.RootHash)
}

// tryDecodeValue will check whether the Value (V) can be decoded from the received
// shards. If the decode was successfull output will be set the this value.
func (rbcRef *RBC) tryDecodeValue(hash []byte) error {
	if rbcRef.outputDecoded || rbcRef.countReadys(hash) <= 2*rbcRef.F || rbcRef.countEchos(hash) <= rbcRef.F {
		return nil
	}
	// At this point we can decode the shards. First we create a new slice of
	// only sortable proof values.
	rbcRef.outputDecoded = true
	var proof proofs
	for _, echo := range rbcRef.recvEchos {
		proof = append(proof, echo.ProofRequest)
	}
	sort.Sort(proof)

	// Reconstruct the value with reedsolomon encoding.
	shards := make([][]byte, rbcRef.numParityShards+rbcRef.numDataShards)
	for _, index := range proof {
		shards[index.Index] = index.Proof[0]
	}
	if err := rbcRef.encoder.Reconstruct(shards); err != nil {
		return nil
	}
	var value []byte
	for _, data := range shards[:rbcRef.numDataShards] {
		value = append(value, data...)
	}
	rbcRef.output = value
	return nil
}

// countEchos count the number of echos with the given hash.
func (rbcRef *RBC) countEchos(hashcode []byte) int {
	n := 0
	for _, e := range rbcRef.recvEchos {
		if bytes.Compare(hashcode, e.RootHash) == 0 {
			n++
		}
	}
	return n
}

// countReadys count the number of readys with the given hash.
func (rbcRef *RBC) countReadys(hashcode []byte) int {
	n := 0
	for _, h := range rbcRef.recvReadys {
		if bytes.Compare(hashcode, h) == 0 {
			n++
		}
	}
	return n
}

// MakeProofRequests will build a merkletree out of the given shards and make
// equal ProofRequest to send one proof to each participant in the consensus.
func MakeProofRequests(shards [][]byte) ([]*ProofRequest, error) {
	request := make([]*ProofRequest, len(shards))
	for i := 0; i < len(request); i++ {
		tree := merkletree.New(sha256.New())
		tree.SetIndex(uint64(i))
		for i := 0; i < len(shards); i++ {
			tree.Push(shards[i])
		}
		root, proof, proofIndex, n := tree.Prove()
		request[i] = &ProofRequest{
			RootHash: root,
			Proof:    proof,
			Index:    int(proofIndex),
			Leaves:   int(n),
		}
	}
	return request, nil
}

// makeProofRequests will build a merkletree out of the given shards and make
// equal ProofRequest to send one proof to each participant in the consensus.
func MakeBroadcastMessages(shards [][]byte) ([]*BroadcastMessage, error) {
	broadcastMessageDeck := make([]*BroadcastMessage, len(shards))
	for i := 0; i < len(broadcastMessageDeck); i++ {
		tree := merkletree.New(sha256.New())
		tree.SetIndex(uint64(i))
		for i := 0; i < len(shards); i++ {
			tree.Push(shards[i])
		}
		root, proof, proofIndex, n := tree.Prove()
		broadcastMessageDeck[i] = &BroadcastMessage{
			Payload: &ProofRequest{
				RootHash: root,
				Proof:    proof,
				Index:    int(proofIndex),
				Leaves:   int(n),
			},
		}
	}
	return broadcastMessageDeck, nil
}

// ValidateProof will validate the given ProofRequest and hence return true or
// false accordingly.
func ValidateProof(request *ProofRequest) bool {
	return merkletree.VerifyProof(
		sha256.New(),
		request.RootHash,
		request.Proof,
		uint64(request.Index),
		uint64(request.Leaves))
}

// MakeShards will split the given value into equal sized shards along with
// somen additional parity shards.
func MakeShards(encoder reedsolomon.Encoder, data []byte) ([][]byte, error) {
	shards, err := encoder.Split(data)
	if err != nil {
		return nil, err
	}
	if err := encoder.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

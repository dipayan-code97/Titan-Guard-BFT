package main

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/dipayan-code97/hbbft"
)

const (
	NODAL_LENGTH = 4
	BATCH_SIZE   = 500
	CORES        = 4
)

type Message struct {
	sender  uint64
	payload hbbft.MessageTuple
}

var (
	textDelay    = (3 * time.Millisecond) / CORES
	messages     = make(chan Message, 1024*1024)
	relayChannel = make(chan *Transaction, 1024*1024)
)

func main() {
	var (
		nodes = ConstructNetwork(NODAL_LENGTH)
	)
	for _, node := range nodes {
		go node.run()
		go func(node *Server) {
			if err := node.honeyBadger.start(); err != nil {
				log.Fatal(err)
			}
			for _, msg := range node.honeyBadger.messages() {
				messages <- Message{node.serverID, msg}
			}
		}(node)
	}

	// handle the relayed transactions.
	go func() {
		for text := range relayChannel {
			for _, node := range nodes {
				node.addTransactions(text)
			}
		}
	}()

	for {
		msg := <-messages
		node := nodes[msg.payload.To]
		switch textPayload := msg.payload.Payload.(type) {
		case hbbft.HBMessage:
			if err := node.honeyBadger.HandleMessage(msg.sender, textPayload.Epoch, textPayload.Payload.(*hbbft.ACSMessage)); err != nil {
				log.Fatal(err)
			}
			for _, msg := range node.honeyBadger.Messages() {
				messages <- Message{node.serverID, msg}
			}
		}
	}
}

// Server represents the local node.
type Server struct {
	serverID    uint64
	honeyBadger *hbbft.HoneyBadger
	transport   hbbft.Transport
	rpcChannel  <-chan hbbft.RPC
	rwLocker    sync.RWMutex
	memoryPool  map[string]*Transaction
	totalCommit int
	start       time.Time
}

func NewServer(id uint64, transport hbbft.Transport, nodes []uint64) *Server {
	honeyBadger := hbbft.NewHoneyBadger(hbbft.Config{
		N:         len(nodes),
		ID:        id,
		Nodes:     nodes,
		BatchSize: BATCH_SIZE,
	})
	return &Server{
		serverID:    id,
		transport:   transport,
		honeyBadger: honeyBadger,
		rpcChannel:  transport.Consume(),
		memoryPool:  make(map[string]*Transaction),
		start:       time.Now(),
	}
}

// Simulate the delay of verifying a transaction.
func (serverRef *Server) verifyTransaction(textContent *Transaction) bool {
	time.Sleep(textDelay)
	return true
}

func (serverRef *Server) addTransactions(textContent ...*Transaction) {
	for _, text := range textContent {
		if serverRef.verifyTransaction(text) {
			serverRef.rwLocker.Lock()
			serverRef.memoryPool[string(text.hashcode())] = text
			serverRef.rwLocker.Unlock()

			// Add this transaction to the hbbft buffer.
			serverRef.honeyBadger.AddTransaction(text)
			// relay the transaction to all other nodes in the network.
			go func() {
				for indexer := 0; indexer < len(serverRef.honeyBadger.Nodes); indexer++ {
					if uint64(indexer) != serverRef.honeyBadger.ID {
						relayChannel <- text
					}
				}
			}()
		}
	}
}

// Loop that is creating bunch of random transactions.
func (serverRef *Server) textLoop() {
	timer := time.NewTicker(1 * time.Second)
	for {
		<-timer.C
		serverRef.addTransactions(MakeTransactions(1000)...)
	}
}

func (serverRef *Server) commitLoop() {
	timer := time.NewTicker(time.Second * 2)
	committed := make(map[string]struct{}, 1000)
	for {
		select {
		case <-timer.C:
			output := serverRef.honeyBadger.Outputs()
			var n int
			serverRef.rwLocker.Lock()
			for _, ith := range output {
				for _, jth := range ith {
					hash := jth.hashcode()
					if _, ok := committed[string(hash)]; !ok {
						// Transaction is not in our mempool which implies we
						// need to do verification.
						if _, ok := serverRef.memoryPool[string(hash)]; !ok {
							func() bool {
								time.Sleep(textDelay)
								return true
							}()
						}
						delete(serverRef.memoryPool, string(hash))
						n++
					}
				}
			}
			serverRef.totalCommit += n
			serverRef.rwLocker.Unlock()
			committed = make(map[string]struct{}, 1000)
			delta := time.Since(serverRef.start)
			if serverRef.serverID == 1 {
				fmt.Println("")
				fmt.Println("===============================================")
				fmt.Printf("SERVER (%d)\n", serverRef.serverID)
				fmt.Printf("commited %d transactions over %v\n", serverRef.totalCommit, delta)
				fmt.Printf("throughput %d TX/s\n", serverRef.totalCommit/int(delta.Seconds()))
				fmt.Println("===============================================")
				fmt.Println("")
			}
		}
	}
}

func (serverRef *Server) run() {
	go serverRef.textLoop()
	go serverRef.commitLoop()
}

func ConstructNetwork(numOfNodes int) []*Server {
	transports := make([]hbbft.Transport, numOfNodes)
	nodes := make([]*Server, numOfNodes)
	for edge := 0; edge < numOfNodes; edge++ {
		transports[edge] = hbbft.NewLocalTransport(uint64(edge))
		nodes[edge] = NewServer(uint64(edge), transports[edge], MakeIndices(numOfNodes))
	}
	ConnectTransports(transports)
	return nodes
}

func ConnectTransports(transports []hbbft.Transport) {
	if len(transports) < 2 {
		return
	}

	centralTransport := transports[0]
	for i := 1; i < len(transports); i++ {
		centralTransport.Connect(transports[i].Addr(), transports[i])
		transports[i].Connect(centralTransport.Addr(), centralTransport)
	}
}

func MakeIndices(n int) []uint64 {
	indices := make([]uint64, n)
	for index := 0; index < n; index++ {
		indices[index] = uint64(index)
	}
	return indices
}

// Transaction represents a transacion -\_(^_^)_/-.
type Transaction struct {
	Nonce uint64
}

func NewTransaction() *Transaction {
	return &Transaction{rand.Uint64()}
}

// Hash implements the hbbft.Transaction interface.
func (transactionRef *Transaction) hashcode() []byte {
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, transactionRef.Nonce)
	return buffer
}

func MakeTransactions(n int) []*Transaction {
	transaction := make([]*Transaction, n)
	for index := 0; index < n; index++ {
		transaction[index] = NewTransaction()
	}
	return transaction
}

func Init() {
	// logrus.SetLevel(logrus.DebugLevel)
	rand.Seed(time.Now().UnixNano())
	gob.Register(&Transaction{})
}

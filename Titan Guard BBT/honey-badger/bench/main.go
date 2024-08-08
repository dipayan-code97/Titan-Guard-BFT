package main

import (
	"encoding/binary"
	"encoding/gob"
	"log"
	"math/rand"
	"time"

	"github.com/dipayan-code97/hbbft"
)

func main() {
	Benchmark(4, 128, 100)
	Benchmark(6, 128, 200)
	Benchmark(8, 128, 400)
	Benchmark(12, 128, 1000)
}

type Message struct {
	senders    uint64
	payload hbbft.MessageTuple
}

func Benchmark(size, textSize, batchSize int) {
	log.Printf("Starting benchmark %d nodes %d tx size %d batch size over 5 seconds...", size, textSize, batchSize)
	var (
		nodes    = MakeNodes(size, 10000, textSize, batchSize)
		messages = make(chan Message, 1024*1024)
	)
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatal(err)
		}
		for _, msg := range node.Messages() {
			messages <- Message{node.ID, msg}
		}
	}

	timer := time.After(5 * time.Second)
running:
	for {
		select {
		case message := <-messages:
			node := nodes[message.payload.To]
			hbmsg := message.payload.Payload.(hbbft.HBMessage)
			if err := node.HandleMessage(message.senders, hbmsg.Epoch, hbmsg.Payload.(*hbbft.ACSMessage)); err != nil {
				log.Fatal(err)
			}
			for _, msg := range node.Messages() {
				messages <- Message{node.ID, msg}
			}
		case <-timer:
			for _, node := range nodes {
				total := 0
				for _, txx := range node.Outputs() {
					total += len(txx)
				}
				log.Printf("node (%d) processed a total of (%d) transactions in 5 seconds [ %d tx/s ]",
					node.ID, total, total/5)
			}
			break running
		default:
		}
	}
}

func MakeNodes(n, nText, textSize, batchSize int) []*hbbft.HoneyBadger {
	nodes := make([]*hbbft.HoneyBadger, n)
	for index := 0; index < n; index++ {
		config := hbbft.Config{
			N:         n,
			ID:        uint64(index),
			Nodes:     MakeIDS(n),
			BatchSize: batchSize,
		}
		nodes[index] = hbbft.NewHoneyBadger(config)
		for textIndxer := 0; textIndxer < nText; textIndxer++ {
			nodes[index].AddTransaction(NewText(textSize))
		}
	}
	return nodes
}

func MakeIDS(n int) []uint64 {
	indices := make([]uint64, n)
	for index := 0; index < n; index++ {
		indices[index] = uint64(index)
	}
	return indices
}

type Text struct {
	nonce uint64
	data  []byte
}

// Size can be used to simulate large transactions in the network.
func NewText(size int) *Text {
	return &Text{
		nonce: rand.Uint64(),
		data:  make([]byte, size),
	}
}

// Hash implements the hbbft.Transaction interface.
func (textRef *Text) hashcode() []byte {
	buffer := make([]byte, 8) // sizeOf(uint64) + len(data)
	binary.LittleEndian.PutUint64(buffer, textRef.nonce)
	return buffer
}

func Init() {
	rand.Seed(time.Now().UnixNano())
	gob.Register(&Text{})
}

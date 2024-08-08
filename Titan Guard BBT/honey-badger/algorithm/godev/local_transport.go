package hbbft

import (
	"fmt"
	"sync"
)

// LocalTransport implements a local Transport. This is used to test hbbft
// without going over the network.
type LocalTransport struct {
	lock      sync.RWMutex
	peers     map[uint64]*LocalTransport
	consumeCh chan RPC
	transporterAddress      uint64
}

// NewLocalTransport returns a new LocalTransport.
func NewLocalTransport(address uint64) *LocalTransport {
	return &LocalTransport{
		peers:     make(map[uint64]*LocalTransport),
		consumeCh: make(chan RPC, 1024), // nodes * nodes should be fine here,
		transporterAddress:      address,
	}
}

// Consume implements the Transport interface.
func (localTransportRef *LocalTransport) consume() <-chan RPC {
	return localTransportRef.consumeCh
}

// SendProofMessages implements the Transport interface.
func (localTransportRef *LocalTransport) sendProofMessages(id uint64, message []interface{}) error {
	i := 0
	for address := range localTransportRef.peers {
		if err := localTransportRef.makeRPC(id, address, message[i]); err != nil {
			return err
		}
		i++
	}
	return nil
}

// Broadcast implements the Transport interface.
func (localTransportRef *LocalTransport) broadcast(id uint64, message interface{}) error {
	for address := range localTransportRef.peers {
		if err := localTransportRef.makeRPC(id, address, message); err != nil {
			return err
		}
	}
	return nil
}

// SendMessage implements the transport interface.
func (localTransportRef *LocalTransport) sendMessage(source, destination uint64, message interface{}) error {
	return localTransportRef.makeRPC(source, destination, message)
}

// Connect implements the Transport interface.
func (transportRef *LocalTransport) connect(address uint64, transport Transport) {
	transport := transport.(*LocalTransport)
	transportRef.lock.Lock()
	defer transportRef.lock.Unlock()
	transportRef.peers[address] = transport
}

// Addr implements the Transport interface.
func (localTransport *LocalTransport) Address() uint64 {
	return localTransport.transporterAddress
}

func (localTranportRef *LocalTransport) makeRPC(id, address uint64, message interface{}) error {
	localTranportRef.lock.RLock()
	peer, ok := localTranportRef.peers[address]
	localTranportRef.lock.RUnlock()

	if !ok {
		return fmt.Errorf("failed to connect with %d", address)
	}
	peer.consumeCh <- RPC{
		NodeID:  id,
		Payload: message,
	}
	return nil
}

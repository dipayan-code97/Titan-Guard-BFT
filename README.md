# Titan Guard Byzantine Fault Tolerance
   Titan Guard BFT is a practical implementation of the Honey Badger Byzantine Fault Tolerance (BFT) consensus algorithm written in Go. 
   This package provides the building blocks for integrating a resilient and scalable BFT protocol into your applications.
   The exposed engine can be seamlessly integrated into existing infrastructures, allowing developers the flexibility to
   use the provided transport layer or implement their own.

   This implementation includes the following key sub-protocols:

   # Reliable Broadcast (RBC)
     * Utilizes Reed-Solomon erasure encoding to securely disseminate an encrypted set of transactions across the network.
   # Binary Byzantine Agreement (BBA)
     * Employs a common coin to ensure that a majority of participants reach consensus, confirming the completion of the RBC process.
   # Asynchronous Common Subset (ACS)
     * Integrates RBC and BBA to reach agreement on a set of encrypted transactions among all participants.
   # Titan Guard HoneyBadger Protocol
     * The top-level protocol combines RBC, BBA, and ACS into a complete, production-grade consensus engine, 
       designed to withstand Byzantine faults and asynchronous network conditions.

# Usage
  Install Dependencies
  To install the required dependencies, run:

  shell
  Copy code
  make deps
  Run Tests
  To run the test suite:

  shell
  Copy code
  make test
  Integrating Titan Honey BFT into Your Application
  To integrate the Titan Honey BFT protocol into your setup, follow these steps:

# Create a new instance of Titan Guard Honey Badger BBT : 
 
  // Define a Config struct with your preferred settings.
  cfg := hbbft.Config{
      // The number of nodes in the network.
      N: 4,
      // Identifier of this node.
      ID: 101,
      // Identifiers of the participating nodes.
      Nodes: uint64{67, 1, 99, 101},
      // The preferred batch size. If BatchSize is empty, an ideal batch size will
      // be chosen for you.
      BatchSize: 100,
  }
  
  // Instantiate the HoneyBadger engine using the defined config.
  hb := hbbft.NewHoneyBadger(cfg)
  Add transactions to the engine:
  
# Titan Guard Honey BBT uses an interface to ensure compatibility with various types of transactions.
  The user requirement is that the transaction must implement the Hash() []byte method.

  // Transaction is an interface that abstracts the underlying data of the actual
  // transaction, allowing for easy integration with other applications.
  type Transaction interface {
    Hash() []byte
  }
  
  // Add new transactions to the engine. This method can be called concurrently.
  hb.AddTransaction(tx)

# Start the engine:

  hb.Start() // Begins proposing batches of transactions across the network.

# Consume committed transactions:

  Applications can access committed transactions at their convenience. Once consumed, the output will reset for the next batch.
  
  hb.Outputs() // Returns a map of committed transactions per epoch.
  
  for epoch, tx := range hb.Outputs() {
    fmt.Printf("batch for epoch %d: %v\n", epoch, tx)
  }
  A working implementation can be found in the bench folder, where Titan Guard Honey BFT is implemented over local transport.

# Current Project Features
   * Reliable Broadcast Algorithm
   * Binary Byzantine Agreement
   * Asynchronous Common Subset
   * HoneyBadger top-level protocol

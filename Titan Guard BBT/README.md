

# Summary := BFT is a practical implementation of the Honey Badger Byzantine Fault Tolerance (BFT) consensus algorithm written in Go. 
             This package provides the building blocks for integrating a resilient and scalable BFT protocol into your applications. 
             The exposed engine can be seamlessly integrated into existing infrastructures, allowing developers the flexibility to use the 
             provided transport layer or implement their own.

This implementation includes the following key sub-protocols:

  * Reliable Broadcast (RBC)
  *  Utilizes Reed-Solomon erasure encoding to securely disseminate an encrypted set of transactions across the network.
     Binary Byzantine Agreement (BBA)
Employs a common coin to ensure that a majority of participants reach consensus, confirming the completion of the RBC process.
Asynchronous Common Subset (ACS)
Integrates RBC and BBA to reach agreement on a set of encrypted transactions among all participants.
HoneyBadger Protocol
The top-level protocol combines RBC, BBA, and ACS into a complete, production-grade consensus engine, designed to withstand Byzantine faults and asynchronous network conditions.
# Simple Dynamo

The main goal of Simple Dynamo is to provide both availability and linearizability at the same time. Read and write operations are guaranteed even under failures and reads will return the most recent value. Messages are hashed using SHA-1 and assigned to a node.

## Membership 
All other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node

## Failure Handling 
Failure handling is achieved through chain replication. In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write. A read operation always comes to the last partition and reads the value from the last partition to achieve linearizibility. 

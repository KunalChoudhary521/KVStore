# KVStore
Key-Value Distributed Database System

There are 2 branches to this project:
1. Master: My implementation of Key-value store with improvements to performance and consistent code style
2. ECE419: Key-value store implemented as course work in ECE419 in a team of 3 students


Full Implementation of Key-Value store is separated into 3 Milestones:
1. Milestone #1: Involves 1 server (KVServer) that accepts multiple client (KVClient) requests for get or put commands.
    *  get(string: key): Returns the value corresponding to the key passed in as an argument
    *  put(string: key, string: value): server stores (to disk) the (key, value) pair
    
    
2. Milestone #2: Multiple servers are logically arranged in a ring structure. Keys and server's <IP:port>
                 are hashed using MD5 and consistent hashing is used to help balance the number of 
                 key-value pairs across all servers.                 
    *  An External Configuration Service (ECS) is used to add or remove a server to or from the ring.
    
    
3. Milestone #3: Key-value pairs are replicated across 2 additional servers to increases their availability.
                 Each server acts as a co-ordinator and 2 servers that follow act as replicas to it. Replication is
                 only performed by the co-ordinator; however, any client can retrieve a value from either the
                 replica or the co-orinator.

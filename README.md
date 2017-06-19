# KVStore
## Key-Value Distributed Database System


__There are 2 branches to this project:__
1. __Master:__ My implementation of Key-value store with improvements to performance and consistent code style
2. __ECE419:__ Key-value store implemented as course work in ECE419 in a team of 3 students


Full Implementation of Key-Value store is separated into 3 Milestones:
1. __Milestone #1:__ Involves 1 server (KVServer) that accepts multiple client (KVClient) requests for get or put commands.
    *  __get(string: key):__ Returns the value corresponding to the key passed in as an argument
    *  __put(string: key, string: value):__ server stores (to disk) the (key, value) pair
    
    
2. __Milestone #2:__ Multiple servers are logically arranged in a ring structure. Keys and server's <IP:port>
                 are hashed using MD5 and consistent hashing is used to help balance the number of 
                 key-value pairs across all servers.                 
    *  An External Configuration Service (ECS) is used to add or remove a server to or from the ring.
    
    
3. __Milestone #3:__ Key-value pairs are replicated across 2 additional servers to increases their availability.
                 Each server acts as a co-ordinator and 2 servers that follow act as replicas to it. Replication is
                 only performed by the co-ordinator; however, any client can retrieve a value from either the
                 replica or the co-orinator.

package ecs;

import common.messages.KVAdminMessage;

public interface ECSCommInterface
{
    /**
     * Choose <numberOfNodes> servers from the available machines and start the KVServer.
     * @param numberOfNodes
     *                      number of nodes to start
     * @param cacheSize
     *                      cache size of each KVServer
     * @param strategy
     *                      cache strategy (FIFO, LFU, LRU) of each KVServer
     */
    public KVAdminMessage initService(int numberOfNodes, String logLevel, int cacheSize, String strategy);

    /**
     * Starts the KVServer @ <address:port>, all client requests
     * and all ECS requests are processed.
     */
    public KVAdminMessage start(String address, int port);

    /**
     * Stops the KVServer @ <address:port>, all client requests
     * are rejected and only ECS requests are processed.
     */
    public KVAdminMessage stop(String address, int port);

    /**
     * Stops KVServer @ <address:port> processes.
     */
    public void shutDown(String address, int port);

    /**
     * Create a new KVServer at the next available address:port
     * in ecs.config  with the specified cache size and replacement
     * strategy and add it to the storage service.
     * @param cacheSize
     *                  cache size of each KVServer
     * @param strategy
     *                  cache strategy (FIFO, LFU, LRU) of each KVServer
     */
    public KVAdminMessage addNode(String logLevel, int cacheSize, String strategy);

    /**
     * Remove a server from the storage service ring
     * @param address
     *              address/IP of the KVServer to be removed
     * @param port
     *              port of the KVServer to be removed
     */
    public KVAdminMessage removeNode(String address, int port);
}
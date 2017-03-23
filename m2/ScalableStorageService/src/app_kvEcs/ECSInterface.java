package app_kvEcs;

import app_kvServer.Metadata;

public interface ECSInterface {
    void initService(int numOfServers, int cSize, String strat) throws Exception;

    String addNode(int cacheSize, String strategy);

    void removeNode(String host, int port) throws Exception;

    void start();

    void stop();

    void shutDown();

    void lockWrite(String host, int port);

    void unlockWrite(String host, int port);

    void moveData(Metadata srcServer, Metadata dstServer, String startRange, String endRange);

    void sendViaTCP(String host, int port, byte[] data);
}

package app_kvEcs;

import app_kvServer.Metadata;

public interface ECSInterface {
    public void initService(int numOfServers, int cSize, String strat);
    public void addNode(String host, int port, int cacheSize, String strategy);
    public void removeNode(String host, int port);
    public void start();
    public void stop();
    public void shutDown();
    public void lockWrite(String host, int port);
    public void unlockWrite(String host, int port);
    public void moveData(Metadata dstServer,String startRange, String endRange);
    public void sendViaTCP(String host, int port, byte[] data);
}

package app_kvEcs;

import app_kvServer.Metadata;

public interface ECSInterface {
    public void initKVServer(int numOfServers, int cSize, String strat, boolean log);
    public void addServer(String host, int port, int cacheSize, String strategy, boolean log);
    public void removeServer(String host, int port);
    public void start(String host, int port);
    public void stop(String host, int port);
    public void shutDown(String host, int port);
    public void lockWrite(String host, int port);
    public void unlockWrite(String host, int port);
    public void moveData(String host, int port, String startRange, String endRange);
    public void sendViaTCP(String host, int port, byte[] data);
}

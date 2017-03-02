package app_kvEcs;

import app_kvServer.Metadata;

public interface ECSInterface {
    public void initKVServer(int numOfServers, int cSize, String strat, boolean log);
    public void initService(int numOfServers, int cSize, String strat);
    public void addServer(String host, int port, int cacheSize, String strategy, boolean log);
    public void removeServer(String host, int port);
    public void addNode(int cacheSize, String strategy);
    public void removeNode(String host, int port);
    public void start();
    public void startKVServer(String host, int port);
    public void stop();
    public void stopKVServer(String host, int port);
    public void shutDown();
    public void shutDownKVServer(String host, int port);//todo
    public void lockWrite(String host, int port);
    public void unlockWrite(String host, int port);
    public void moveData(String host, int port, String startRange, String endRange);//todo
    public void sendViaTCP(String host, int port, byte[] data);
}

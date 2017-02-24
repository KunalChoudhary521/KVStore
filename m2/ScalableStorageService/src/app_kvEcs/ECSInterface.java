package app_kvEcs;

import app_kvServer.Metadata;

import java.util.List;

/**
 * Created by Haashir on 2/24/2017.
 */
public interface ECSInterface {
    public void initKVServer(Metadata m, int cacheSize, String stratgy, boolean log);
    public void start(String host, int port);
    public void stop(String host, int port);
    public void shutDown(String host, int port);
    public void lockWrite(String host, int port);
    public void unlockWrite(String host, int port);
    public void moveData(String startRange, String endRange, String host, int port);
    public void update(Metadata newMetadata, String host, int port);
}

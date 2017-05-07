package app_kvServer;

import cache.KVCache;

import java.nio.file.Path;

//Holds variables that are shared between KVServer & ClientConnection objects
public class ServerInfo
{
    private Path kvDirPath;
    private Path mDataFile;
    private KVCache cache;
    private String startHash, endHash;
    private Boolean running;

    public ServerInfo()
    {
        startHash= null;
        endHash = null;
    }

    public void setKvDirPath(Path kvDirPath)
    {
        this.kvDirPath = kvDirPath;
    }

    public void setmDataFile(Path mDataFile)
    {
        this.mDataFile = mDataFile;
    }

    public void setCache(KVCache cache)
    {
        this.cache = cache;
    }

    public void setStartHash(String startHash)
    {
        this.startHash = startHash;
    }

    public void setEndHash(String endHash)
    {
        this.endHash = endHash;
    }

    public void setRunning(Boolean running)
    {
        this.running = running;
    }

    public Path getKvDirPath()
    {
        return kvDirPath;
    }

    public Path getmDataFile()
    {
        return mDataFile;
    }

    public KVCache getCache()
    {
        return cache;
    }

    public String getStartHash()
    {
        return startHash;
    }

    public String getEndHash()
    {
        return endHash;
    }

    public Boolean getRunning()
    {
        return running;
    }
}

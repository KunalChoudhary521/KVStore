package cache;

public interface KVCache
{
    public String checkCache(String k);
    public void insertInCache(String k, String v);
    public boolean deleteFromCache(String k);
}

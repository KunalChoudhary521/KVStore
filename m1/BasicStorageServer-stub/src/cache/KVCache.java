package cache;

public interface KVCache
{
    public String checkCache(String k,boolean log);
    public void insertInCache(String k, String v);
    public void deleteFromCache(String k);
}

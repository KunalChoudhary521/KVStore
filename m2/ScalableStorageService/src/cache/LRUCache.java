package cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LRUCache implements KVCache
{
    private ConcurrentHashMap<String, LRUNode> keyMap;//<String: Key, String: ValNode>
    private ConcurrentLinkedDeque<LRUNode> lruQueue;
    private final int maxCacheSize;

    public LRUCache(int maxSize)
    {
        this.maxCacheSize = maxSize;
        this.keyMap = new ConcurrentHashMap<>();
        this.lruQueue = new ConcurrentLinkedDeque<>();
    }

    public ConcurrentHashMap<String, LRUNode> getKeyMap() { return this.keyMap; }
    public ConcurrentLinkedDeque getLruQueue() { return this.lruQueue; }
    public int getMaxCacheSize() { return this.maxCacheSize; }

    public String checkCache(String k, boolean log)
    {
        if (this.maxCacheSize>0) {

            if ((!this.keyMap.isEmpty()) && this.keyMap.containsKey(k)) {
                LRUNode valNode = keyMap.get(k);

                updateLruQueue(valNode);

                return valNode.getValue();
            }
        }

        return null;
    }

    public void insertInCache(String k, String v)
    {

        if (this.maxCacheSize>0) {
            if ((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k)))//if key is in the cache
            {
                LRUNode oldNode = this.keyMap.get(k);
                oldNode.setValue(v);//update oldNode value
                updateLruQueue(oldNode);
            } else//key not in cache
            {
                LRUNode newValNode = new LRUNode(k, v);
                //Tip for future: to speed up cache, remove a bunch of kv pairs
                if (this.keyMap.size() >= this.maxCacheSize)//cache is full
                {
                    LRUNode ndToEvict = this.lruQueue.getLast();
                    this.lruQueue.remove(ndToEvict);//evict least recently used Node from lruQueue
                    this.keyMap.remove(ndToEvict.getKey());//evict least recently used Node from keyMap
                }
                this.lruQueue.addFirst(newValNode);//insert newly created node
                this.keyMap.put(k, newValNode);//insert newly created node in keyMap
            }
        }
    }

    private void updateLruQueue(LRUNode nd)//move nd to the front of lruQueue
    {
        if (this.maxCacheSize>0) {
            if (!this.lruQueue.getFirst().equals(nd)) {
                this.lruQueue.remove(nd);
                this.lruQueue.addFirst(nd);
            }
        }
    }

    public void deleteFromCache(String k)
    {
        //this function does nothing if key, k, is not found
        if (this.maxCacheSize>0) {
            if ((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k))) {
                LRUNode ndToDelete = this.keyMap.get(k);
                if ((!this.lruQueue.isEmpty()) && (this.lruQueue.contains(ndToDelete))) {
                    this.lruQueue.remove(ndToDelete);//remove from queue
                    this.keyMap.remove(ndToDelete.getKey());//remove from map
                }
            }
        }
    }


    public void printCacheState()
    {
        if (this.maxCacheSize>0) {
            for (LRUNode itr : this.lruQueue) {
                System.out.print(itr.getKey() + ":" + itr.getValue() + "->");
            }
            System.out.println();
        }
    }
}

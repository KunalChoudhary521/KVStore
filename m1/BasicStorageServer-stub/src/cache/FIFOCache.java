package cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class FIFOCache implements KVCache
{
    private ConcurrentHashMap<String, FIFONode> keyMap;//<String: Key, String: ValNode>
    private ConcurrentLinkedDeque<FIFONode> fifoQueue;
    private final int maxCacheSize;

    public FIFOCache(int maxSize)
    {
        this.maxCacheSize = maxSize;
        this.keyMap = new ConcurrentHashMap<>();
        this.fifoQueue = new ConcurrentLinkedDeque<>();
    }

    public ConcurrentHashMap<String, FIFONode> getKeyMap() { return this.keyMap; }
    public ConcurrentLinkedDeque<FIFONode> getFifoQueue() { return this.fifoQueue; }
    public int getMaxCacheSize() { return this.maxCacheSize; }

    public String checkCache(String k)
    {
        if((!this.keyMap.isEmpty()) && this.keyMap.containsKey(k))
        {
            FIFONode valNode = keyMap.get(k);
            /*Unlike LRU, position of node doesn't change if it already exists in the queue*/
            return valNode.getValue();
        }
        return null;
    }

    public void insertInCache(String k, String v)
    {
        if((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k)))//if key already exists in the cache
        {
            FIFONode oldNode = this.keyMap.get(k);
            oldNode.setValue(v);//update oldNode value
            /*Unlike LRU, position of node doesn't change if it already exists in the queue*/
        }
        else//key not in cache
        {
            FIFONode newValNode = new FIFONode(k,v);
            if(this.keyMap.size() >= this.maxCacheSize)//cache is full
            {
                this.keyMap.remove(fifoQueue.getFirst().getKey());//evict head node from keyMap
                fifoQueue.removeFirst();//evict head node from fifoQueue
            }
            fifoQueue.addLast(newValNode);//insert newly created node to the back of fifoQueue
            this.keyMap.put(k, newValNode);//insert newly created node in keyMap
        }
    }

    public void deleteFromCache(String k)
    {
        //this function does nothing if key, k, is not found
        if((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k)))
        {
            FIFONode ndToDelete = this.keyMap.get(k);
            if((!this.fifoQueue.isEmpty()) && (this.fifoQueue.contains(ndToDelete)))
            {
                this.fifoQueue.remove(ndToDelete);
                this.keyMap.remove(ndToDelete.getKey());
            }
        }
    }
    public void printCacheState()
    {
        for(FIFONode itr: this.fifoQueue)
        {
            System.out.print(itr.getKey() + ":" + itr.getValue() + "->");
        }
        System.out.println();
    }
}

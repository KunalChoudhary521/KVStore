package cache;

import java.util.HashMap;
import cache.Node;
import cache.CacheQueue;

public class LRUCache
{
    private HashMap<String, Node> keyMap;//<String: Key, String: ValNode>
    private CacheQueue cQueue;
    private final int maxCacheSize;

    public LRUCache(int maxSize)
    {
        this.maxCacheSize = maxSize;
        this.keyMap = new HashMap<>();
        this.cQueue = new CacheQueue();
    }

    public HashMap<String, Node> getKeyMap() { return this.keyMap; }
    public CacheQueue getCQueue() { return this.cQueue; }
    public int getMaxCacheSize() { return this.maxCacheSize; }

    public String checkCache(String k)
    {
        if(keyMap.containsKey(k))
        {
            Node valNode = keyMap.get(k);

            //if valNode is not at the front, then move it to the front of cQueue
            if(!valNode.equals(cQueue.getHead()))
            {
                cQueue.remove(valNode);
                cQueue.insertToFront(valNode);
            }
            return valNode.getValue(); // return the value regardless
        }
        return null;
    }

    public void insertInCache(String k, String v)
    {
        if(this.keyMap.containsKey(k))//if key already exists in the cache
        {
            if(v.trim().equals("null")){
                System.out.println("deleting");
                Node oldNode = this.keyMap.get(k);
                if (cQueue.getHead() != oldNode) {
                    cQueue.remove(oldNode);
                }
            }else {
                Node oldNode = this.keyMap.get(k);
                oldNode.setValue(v);//update oldNode value
                if (cQueue.getHead() != oldNode) {
                    cQueue.remove(oldNode);
                    cQueue.insertToFront(oldNode);//move old node to the front
                }
            }


        }
        else//key not in cache
        {
            if (v.equals("null")){
                return;
            }
            Node newValNode = new Node(k,v);
            if(this.keyMap.size() >= this.maxCacheSize)//cache is full
            {
                System.out.println("Key: " + cQueue.getTail().getKey() +
                        " Value: " + cQueue.getTail().getValue() + " removed\n");//printing for debugging

                this.keyMap.remove(cQueue.getTail().getKey());//evict least recently used Node from keyMap
                cQueue.remove(cQueue.getTail());//evict least recently used Node from cQueue

                cQueue.insertToFront(newValNode);//insert newly created node in cQueue
            }
            else//cache not full
            {
                cQueue.insertToFront(newValNode);//insert newly created node in cQueue
            }

            this.keyMap.put(k, newValNode);//insert newly created node in keyMap
        }
    }

    public void printCacheState()
    {
        this.cQueue.printList();
    }
}

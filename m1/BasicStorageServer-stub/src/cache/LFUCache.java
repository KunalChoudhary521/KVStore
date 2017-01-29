package cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LFUCache implements KVCache
{
    private ConcurrentHashMap<String, LFUNode> keyMap;//<String: Key, Node: ValNode>; use ConcurrentHashMap
    private ConcurrentHashMap<Integer, ConcurrentLinkedDeque<LFUNode>> freqMap;//use
    private final int maxCacheSize;


    public LFUCache(int maxSize)
    {
        this.maxCacheSize = maxSize;
        this.keyMap = new ConcurrentHashMap<>();
        this.freqMap = new ConcurrentHashMap<>();
    }

    public ConcurrentHashMap<String, LFUNode> getKeyMap() { return this.keyMap; }
    public ConcurrentHashMap<Integer, ConcurrentLinkedDeque<LFUNode>> getFreqList() { return this.freqMap;}
    public int getMaxCacheSize() { return this.maxCacheSize; }

    public String checkCache(String k,boolean log)
    {
        if (this.maxCacheSize>0) {
            if (log) {
                System.out.println("LFU");
                System.out.println("empty=" + this.keyMap.isEmpty());
                System.out.println("contains =" + keyMap.containsKey(k));
            }
            if ((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k))) {
                if (log) {
                    System.out.println("getting");
                }
                LFUNode valNode = keyMap.get(k);
                if (log) {
                    System.out.println("valnode=" + valNode);
                }
                String cacheValue = valNode.getValue();//store before removing valNode from valueList
                if (log) {
                    System.out.println("cachevalue" + cacheValue);
                }
                updateFrequency(valNode);
                if (log) {
                    System.out.println("frequency updated");
                }

                return cacheValue;
            }
        }
        if(log) {
            System.out.println("returning null");
        }
        return null;
    }

    public void insertInCache(String k, String v)
    {
        if (this.maxCacheSize>0) {
            if ((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k)))//if key already exists in the cache
            {
                LFUNode oldNode = this.keyMap.get(k);
                oldNode.setValue(v);//update oldNode value
                updateFrequency(oldNode);
            } else//key not in cache
            {
                LFUNode newValNode = new LFUNode(k, v);

                if (this.keyMap.size() >= this.maxCacheSize)//cache is full
                {
                    //find a non-empty least frequently used valuesList
                    int lowestFrq = 1;
                    while (!this.freqMap.containsKey(lowestFrq)) {
                        lowestFrq++;
                    }
                    ConcurrentLinkedDeque<LFUNode> valList = this.freqMap.get(lowestFrq);
                    valList.removeFirst();//remove 1st item because I add from the back
                }
                //check if frequency of 1 exists
                if ((this.freqMap.isEmpty()) || (!this.freqMap.containsKey(1))) {
                    this.freqMap.put(1, new ConcurrentLinkedDeque<LFUNode>());//adds frequency of 1
                }

                //add newValNode to frequency of 1
                this.freqMap.get(1).add(newValNode);//adds to the end of the list
                newValNode.setNodeFrq(1);

                this.keyMap.put(k, newValNode);//add node to keymap
            }
        }
    }

    public void updateFrequency(LFUNode ndToMove)
    {
        if (this.maxCacheSize>0) {
            int ndFreq = ndToMove.getNodeFrq();
            int updateFrq = ndFreq + 1;

            if (!this.freqMap.containsKey(updateFrq))//if a new, higher frequency k,v doesn't exist
            {
                this.freqMap.put(updateFrq, new ConcurrentLinkedDeque<LFUNode>());//a new, higher frequency k,v is added
            }

            ndToMove.setNodeFrq(updateFrq);
            this.freqMap.get(ndFreq).remove(ndToMove);//remove from lower frequency

            if (this.freqMap.get(ndFreq).isEmpty()) {
                //remove a frequency (k-v pair), its list of LFUNode is empty
                this.freqMap.remove(ndFreq);
                //Now size of freqMap is the same as # of k,v pairs in the cache
            }

            this.freqMap.get(updateFrq).add(ndToMove);//add to higher frequency
        }
    }

    public void deleteFromCache(String k)
    {

        //this function does nothing if key, k, is not found
        if (this.maxCacheSize>0) {
            if ((!this.keyMap.isEmpty()) && (this.keyMap.containsKey(k))) {
                LFUNode ndToDelete = this.keyMap.get(k);
                if ((!this.freqMap.isEmpty()) && (this.freqMap.containsKey(ndToDelete.getNodeFrq()))) {
                    ConcurrentLinkedDeque<LFUNode> list = this.freqMap.get(ndToDelete.getNodeFrq());
                    if ((!list.isEmpty()) && (list.contains(ndToDelete))) {
                        list.remove(ndToDelete);
                        this.keyMap.remove(ndToDelete.getKey());

                    /*Remove a frequency (k-v pair), its list of LFUNode is empty.
                      This means ndToDelete was the only node with that frequency*/

                        if (this.freqMap.isEmpty()) {
                            this.freqMap.remove(ndToDelete.getNodeFrq());
                        }
                    }
                }
            }
        }
    }

    public void printCacheState()
    {
        if (this.maxCacheSize>0) {
            int frequency = 1;
            while (this.freqMap.containsKey(frequency)) {
                System.out.print(frequency + ":: ");
                for (LFUNode valNodeItr : this.freqMap.get(frequency)) {
                    System.out.print(valNodeItr.getKey() + ":" + valNodeItr.getValue() + "| ");
                }
                System.out.println("");
                frequency++;
            }
            System.out.println("**********************");
        }
    }
}

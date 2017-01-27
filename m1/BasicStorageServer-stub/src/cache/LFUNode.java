package cache;

public class LFUNode
{
    private String value;
    private String key;//don't need this b/c Hashmap stores key
    private int nodeFrq;

    public LFUNode(String userK, String userV)
    {
        this.key = userK;
        this.value = userV;
        this.nodeFrq = 0;
    }
    public String getValue() { return this.value;}
    public void setValue(String newVal) { this.value = newVal; }

    public String getKey() { return this.key;}
    public void setKey(String newKey) { this.key = newKey; }//not sure if user should be allowed to change this

    public int getNodeFrq() { return this.nodeFrq; }
    public void setNodeFrq(int frq) { this.nodeFrq = frq; }

    public LFUNode clone()
    {
        /*nodeFrq field will be changed outside this function
          nodeFrq is set to 0
        */
        return new LFUNode(this.getKey(), this.getValue());
    }
}

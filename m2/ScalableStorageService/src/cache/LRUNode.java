package cache;

public class LRUNode
{
    private String value;
    private String key;

    public LRUNode(String userK, String userV)
    {
        this.key = userK;
        this.value = userV;
    }

    public String getValue() { return this.value;}
    public void setValue(String newVal) { this.value = newVal; }

    public String getKey() { return this.key;}
    public void setKey(String newKey) { this.key = newKey; }//not sure if user should be allowed to change this

    public boolean equals(LRUNode rhsNode)
    {
        return(this.value.equals(rhsNode.value)) && (this.key.equals(rhsNode.key));
    }
}

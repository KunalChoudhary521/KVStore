package cache;

public class FIFONode
{
    private String value;
    private String key;

    public FIFONode(String userK, String userV)
    {
        this.key = userK;
        this.value = userV;
    }
    public String getValue() { return this.value;}
    public void setValue(String newVal) { this.value = newVal; }

    public String getKey() { return this.key;}
    public void setKey(String newKey) { this.key = newKey; }//not sure if user should be allowed to change this

}

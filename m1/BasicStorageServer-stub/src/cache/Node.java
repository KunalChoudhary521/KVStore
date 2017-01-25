package cache;

public class Node
{
    private String value;
    private String key;
    private Node next;
    private Node prev;

    public Node(String userK, String userV)
    {
        this.key = userK;
        this.value = userV;
        this.next = null;
        this.prev = null;
    }

    public Node getNext() { return this.next; }
    public void setNext(Node nxt){ this.next = nxt; }

    public Node getPrev() { return this.prev; }
    public void setPrev(Node prv) { this.prev = prv; }

    public String getValue() { return this.value;}
    public void setValue(String newVal) { this.value = newVal; }

    public String getKey() { return this.key;}
    public void setKey(String newKey) { this.key = newKey; }//not sure if user should be allowed to change this

    public boolean equals(Node rhsNode)
    {
        if((this.prev == rhsNode.prev) && (this.next == rhsNode.next) && (this.value.equals(rhsNode.value)))
        {
            return true;
        }
        return false;
    }
}

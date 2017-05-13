package common;

public class Metadata {

    public String address;
    public int port;
    public String startHash;
    public String endHash;

    public Metadata(String addr, int port, String startHash, String endHash)
    {
        this.address = addr;
        this.port = port;
        this.startHash = startHash;
        this.endHash = endHash;
    }
    public Metadata(String host, int port)
    {
        this.address = host;
        this.port = port;
        this.startHash = null;
        this.endHash = null;
    }
}

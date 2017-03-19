package app_kvServer;

public class Metadata {

    public String host;
    public String port;
    public String startHash_g;
    public String startHash_p;
    public String endHash;

    public Metadata(String host, String port, String startHash_g, String startHash_p, String endHash)
    {
        this.host = host;
        this.port = port;
        this.startHash_g = startHash_g;
        this.startHash_p = startHash_p;
        this.endHash = endHash;
    }
    public Metadata(String host, String port)
    {
        this.host = host;
        this.port = port;
        this.startHash_g = null;
        this.startHash_p = null;
        this.endHash = null;
    }
}

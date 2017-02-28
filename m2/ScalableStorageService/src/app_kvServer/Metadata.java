package app_kvServer;

public class Metadata {

    public String host;
    public String port;
    public String startHash;
    public String endHash;

    public Metadata(String host, String port, String startHash, String endHash)
    {
        this.host = host;
        this.port = port;
        this.startHash = startHash;
        this.endHash = endHash;
    }
    public Metadata(String host, String port)
    {
        this.host = host;
        this.port = port;
        this.startHash = null;
        this.endHash = null;
    }
}

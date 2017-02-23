package app_kvServer;

/**
 * Created by Haashir on 2/23/2017.
 */
public class Metadata {

    public String host;
    public String port;
    public String startHash;
    public String endHash;

    public Metadata(String host, String port, String startHash, String endHash){
        this.host = host;
        this.port = port;
        this.startHash = startHash;
        this.endHash = endHash;
    }
}

package common.messages;


public class RespToECS implements KVAdminMessage
{
    private String kvServerAddress;
    private int kvServerPort;
    private StatusType type;

    public RespToECS(String address, int port, StatusType typ)
    {
        this.kvServerAddress = address;
        this.kvServerPort = port;
        this.type = typ;
    }

    @Override
    public String getAddress() { return this.kvServerAddress; }

    @Override
    public int getPort() { return this.kvServerPort; }

    @Override
    public StatusType getStatus() { return this.type; }
}

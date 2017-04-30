package common.messages;

public class ResponseMsg implements KVMessage
{
    private String key;
    private String value;
    private StatusType type;

    public ResponseMsg(String key, String value, StatusType type)
    {
        this.key = key;
        this.value = value;
        this.type = type;
    }

    @Override
    public String getKey() {return key;}

    @Override
    public String getValue() {return value;}

    @Override
    public StatusType getStatus() {return type;}
}

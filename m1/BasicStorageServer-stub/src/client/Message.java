package client;

import common.messages.KVMessage;

/**
 * Created by Haashir on 1/24/2017.
 */
public class Message implements KVMessage{

    private String key;
    private String value;
    private StatusType type;

    public Message(String key, String value, StatusType type){
        this.key = key;
        this.value = value;
        this.type = type;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return type;
    }
}

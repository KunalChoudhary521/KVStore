package common.messages;

//analogous to KVMessage.java
public interface KVAdminMessage
{
    public enum StatusType {
        INIT_SUCCESS,
        INIT_ERROR,
        START_SUCCESS,
        START_ERROR,
        STOP_SUCCESS,
        STOP_ERROR,
        ADD_SUCCESS,
        ADD_ERROR,
        REMOVE_SUCCESS,
        REMOVE_ERROR
    }

    /**
     * @return  KVServer's address that is associated with this message,
     */
    public String getAddress();

    /**
     * @return  KVServer's port that is associated with this message
     */
    public int getPort();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();
}

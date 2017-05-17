package client;

import common.Metadata;
import common.TextMessage;
import common.md5;
import common.messages.KVMessage;
import common.messages.RespToClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

public class KVStore implements KVCommInterface
{
    private Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    private String serverAddress;
    private int serverPort;
    private Socket clientSocket;
    private OutputStream output;//to receive messages from KVServer
    private InputStream input;//to send messages to KVServer
    private TreeMap<BigInteger, Metadata> hashRing;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port)
	{
	    this.serverAddress = address;
	    this.serverPort = port;
	}
	
	@Override
	public void connect() throws IOException
    {
        this.clientSocket = new Socket(this.serverAddress,this.serverPort);
        output = this.clientSocket.getOutputStream();
        input = this.clientSocket.getInputStream();
        logger.info("Connection established");
	}

	@Override
	public void disconnect()
    {
        logger.info("trying to close connection ...");

        try
        {
            if (clientSocket != null)
            {
                //input.close();
                //output.close();
                clientSocket.close();
                clientSocket = null;
                logger.info("connection closed!");
            }
        }
        catch (Exception ex)
        {
            logger.error("Unable to close connection!");
        }
	}

	@Override
	public KVMessage put(String key, String value) throws Exception
    {
        if(key.length() > 20 || value.length() > (120*1024))
        {
            //key or value is beyond max length
            logger.error("Error! Max Key length = 20Bytes and Max Value length = 120Bytes");
            return new RespToClient(key, null, KVMessage.StatusType.PUT_ERROR);
        }
        else if (key.startsWith(",") || value.startsWith(","))
        {
            logger.error("Error! key and value MUST NOT start with a comma (,)");
            return new RespToClient(key, value, KVMessage.StatusType.PUT_ERROR);
        }

        String msg = "PUT," + key + "," + value;
        sendMessage(new TextMessage(msg));//marshalling of put message

        TextMessage putResponse = receiveMessage();
        if(putResponse.getMsg().equals("PUT_SUCCESS"))
        {
            return new RespToClient(key, value, KVMessage.StatusType.PUT_SUCCESS);
        }
        else if(putResponse.getMsg().equals("PUT_ERROR"))
        {
            return new RespToClient(key, null, KVMessage.StatusType.PUT_ERROR);
        }
        else if(putResponse.getMsg().equals("DELETE_SUCCESS"))
        {
            return new RespToClient(key, null, KVMessage.StatusType.DELETE_SUCCESS);
        }
        else if(putResponse.getMsg().equals("DELETE_ERROR"))
        {
            return new RespToClient(key, null, KVMessage.StatusType.DELETE_ERROR);
        }
        else if(putResponse.getMsg().equals("SERVER_WRITE_LOCK"))
        {
            return new RespToClient(key, null, KVMessage.StatusType.SERVER_WRITE_LOCK);
        }
        else if(putResponse.getMsg().equals("SERVER_NOT_RESPONSIBLE"))
        {
            return ResendRequest(key, value, "PUT");
            //return new RespToClient(key, null, KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }
        else
        {
            return new RespToClient(key, null, KVMessage.StatusType.PUT_ERROR);
        }
	}

	@Override
	public KVMessage get(String key) throws Exception
    {
        if(key.length() > 20)
        {
            //this key will NOT exist at any KVServer because it will not be stored in the first place
            logger.error("Error! Max Key length = 20Bytes");
            return new RespToClient(key, null, KVMessage.StatusType.GET_ERROR);
        }
        else if (key.startsWith(","))
        {
            //this key will NOT exist at any KVServer because it will not be stored in the first place
            logger.error("Error! key MUST NOT start with a comma (,)");
            return new RespToClient(key, null, KVMessage.StatusType.GET_ERROR);
        }

        String msg = "GET," + key;
        sendMessage(new TextMessage(msg));//marshalling of get message

        TextMessage getResponse = receiveMessage();
        if(getResponse.getMsg().equals("GET_ERROR"))
        {
            logger.error("Error! GET FAILED from KVServer: <" +
                    clientSocket.getInetAddress().getHostAddress() + ":" +
                    clientSocket.getPort() + ">");
            return new RespToClient(key, null, KVMessage.StatusType.GET_ERROR);

        }
        else if(getResponse.getMsg().equals("SERVER_NOT_RESPONSIBLE"))
        {
            return ResendRequest(key, null, "GET");
            //return new RespToClient(key, null, KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }
        else if(getResponse.getMsg().equals("SERVER_STOPPED"))
        {
            return new RespToClient(key, null, KVMessage.StatusType.SERVER_STOPPED);
        }
        else
        {
            return new RespToClient(key, getResponse.getMsg(), KVMessage.StatusType.GET_SUCCESS);
        }
	}

    private KVMessage ResendRequest(String key, String value, String requestType) throws Exception
    {
        KVMessage.StatusType status;
        if(value == null && requestType.equals("PUT")) {
            status = KVMessage.StatusType.DELETE_ERROR;
        } else if(requestType.equals("PUT")) {
            status = KVMessage.StatusType.PUT_ERROR;
        } else if(value == null && requestType.equals("GET")) {
            status = KVMessage.StatusType.GET_ERROR;
        } else {
            status = null;
        }

        sendMessage(new TextMessage("SEND_METADATA"));//part2 of SERVER_NOT_RESPONSIBLE protocol

        TextMessage metaData = receiveMessage();
        if(metaData.getMsg().equals("NO_METADATA")) {
            return new RespToClient(key, null, status);
        } else {
            //find the correct KVServer and re-send put request
            createHashRing(metaData.getMsg());
            Metadata serverInfo = findCorrectServer(key);
            if(serverInfo == null) {
                return new RespToClient(key, null, status);
            } else {
                disconnect();//disconnect from KVServer the client is currently connected to

                this.serverAddress = serverInfo.address;
                this.serverPort = serverInfo.port;

                connect();//connect to the correct KVServer

                //call the appropriate get or put function
                return (requestType.equals("PUT") ? (put(key, value)) : get(key));
            }
        }
    }

    private void createHashRing(String metaDataMsg) throws IOException, NoSuchAlgorithmException
    {
        this.hashRing = new TreeMap<>();
        String[] metaDataLines = metaDataMsg.split(",");

        for (int i = 0; i < metaDataLines.length; i++)
        {
            String[] line = metaDataLines[i].split(";");
            hashRing.put(md5.HashInBI(line[0] + ":" + line[1]),
                    new Metadata(line[0], Integer.parseInt(line[1]),line[2], line[3]));
        }
    }

    /**
     * Find the correct KVServer's address & port for the given key
     * @param key   the key used to find the correct KVServer
     * @return      returns {address, port} if range is found, null otherwise
     */
    private Metadata findCorrectServer(String key) throws NoSuchAlgorithmException
    {
        BigInteger keyHash = md5.HashInBI(key);

        if(hashRing.isEmpty()) { return null; }

        /*
            Return the server that has the next highest hash to the keyHash.
            If keyHash has a hash higher than all KVServers, then return
            the Metadata of KVServer with the lowest hash (due to wrap-around).
         */
        return (hashRing.higherEntry(keyHash) != null) ?
                        hashRing.higherEntry(keyHash).getValue() : hashRing.firstEntry().getValue();
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SENT TO \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg() +"'");
    }

    private TextMessage receiveMessage() throws IOException
    {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

			/* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

			/* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

		/* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("RECEIVED FROM \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">:"
                + msg.getMsg().trim() + "'");
        return msg;
    }
}

package client;


import common.messages.KVMessage;
import common.messages.ResponseMsg;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class KVStore implements KVCommInterface
{
    private Logger logger = Logger.getRootLogger();
    private boolean running;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    private String serverAddress;
    private int serverPort;
    private Socket clientSocket;
    private OutputStream output;//to receive messages from KVServer
    private InputStream input;//to send messages to KVServer
	
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

    public boolean isRunning() {
        return this.running;
    }

    public void setRunning(boolean run) {
        this.running = run;
    }
	
	@Override
	public void connect() throws Exception
    {
        this.clientSocket = new Socket(this.serverAddress,this.serverPort);
        setRunning(true);
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
            setRunning(false);
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
        String msg = "PUT," + key + "," + value;
        TextMessage putRequest = new TextMessage(msg);//marshalling of put message
        boolean isSent = sendMessage(putRequest);

        TextMessage putResponse;
        if(isSent)
        {
            putResponse = receiveMessage();
            if(putResponse.getMsg().equals("PUT_SUCCESS"))
            {
                return new ResponseMsg(key, value, KVMessage.StatusType.PUT_SUCCESS);
            }
            else if(putResponse.getMsg().equals("PUT_ERROR"))
            {
                return new ResponseMsg(key, null, KVMessage.StatusType.PUT_ERROR);
            }
            else if(putResponse.getMsg().equals("DELETE_SUCCESS"))
            {
                return new ResponseMsg(key, null, KVMessage.StatusType.DELETE_SUCCESS);
            }
            else if(putResponse.getMsg().equals("DELETE_ERROR"))
            {
                return new ResponseMsg(key, null, KVMessage.StatusType.DELETE_ERROR);
            }
        }
        else
        {
            logger.error("Error! PUT FAILED: key-value pair NOT SENT to KVServer: <" +
                    clientSocket.getInetAddress().getHostAddress() + ":" +
                    clientSocket.getLocalPort() + ">");
        }
        return new ResponseMsg(key, null, KVMessage.StatusType.PUT_ERROR);
	}

	@Override
	public KVMessage get(String key) throws Exception
    {
        String msg = "GET," + key;
        TextMessage getRequest = new TextMessage(msg);//marshalling of get message
        boolean isSent = sendMessage(getRequest);

        TextMessage getResponse;
        if(isSent)
        {
            getResponse = receiveMessage();
            if(!getResponse.getMsg().trim().equals("GET_ERROR"))
            {
                return new ResponseMsg(key, getResponse.getMsg(), KVMessage.StatusType.GET_SUCCESS);
            }
            else
            {
                logger.error("Error! GET FAILED from KVServer: <" +
                        clientSocket.getInetAddress().getHostAddress() + ":" +
                        clientSocket.getPort() + ">");
            }
        }
        else
        {
            logger.error("Error! GET FAILED: key NOT SENT to KVServer: <" +
                    clientSocket.getInetAddress().getHostAddress() + ":" +
                    clientSocket.getPort() + ">");
        }

        return new ResponseMsg(key, null, KVMessage.StatusType.GET_ERROR);
	}

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    //TODO make this sendMessage symmetric to sendMessage in ClientConnection.java
    public boolean sendMessage(TextMessage msg)
    {
        try
        {
            byte[] msgBytes = msg.getMsgBytes();
            output.write(msgBytes, 0, msgBytes.length);
            output.flush();
            logger.info("SENT TO \t<"
                    + clientSocket.getInetAddress().getHostAddress() + ":"
                    + clientSocket.getPort() + ">: '"
                    + msg.getMsg() +"'");
            return true;
        }
        catch (Exception ex)
        {
            return false;
        }
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
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }
}

package client;


import common.messages.KVMessage;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class KVStore implements KVCommInterface
{
    private Logger logger = Logger.getRootLogger();
    private boolean running;

    private String serverAddress;
    private int serverPort;
    private Socket clientSocket;
    private OutputStream receiveStream;
    private InputStream sendStream;
	
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
		// TODO Auto-generated method stub
        try
        {
            this.clientSocket = new Socket(this.serverAddress,this.serverPort);
            setRunning(true);
            receiveStream = this.clientSocket.getOutputStream();
            sendStream = this.clientSocket.getInputStream();
            logger.info("Connection established");
        }
        catch (Exception ex)
        {
            logger.info("Connection with " + this.serverAddress +
                        ":" + this.serverPort + " not established");
        }
	}

	@Override
	public void disconnect()
    {
		// TODO Auto-generated method stub
        logger.info("trying to close connection ...");

        try
        {
            setRunning(false);
            if (clientSocket != null)
            {
                //sendStream.close();
                //receiveStream.close();
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception
    {
		// TODO Auto-generated method stub
		return null;
	}

	
}

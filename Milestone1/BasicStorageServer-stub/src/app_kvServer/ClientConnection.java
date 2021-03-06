package app_kvServer;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

import cache.KVCache;
import common.md5;

import org.apache.log4j.*;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static final String msgSeparator = ",";
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private Path kvDirPath;
	private KVCache kvCache;
	
	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, Path kvDir, KVCache cache)
    {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.kvDirPath = kvDir;
        this.kvCache = cache;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run()
    {
		try
        {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while(isOpen)
            {
				try
                {
					TextMessage latestMsg = receiveMessage();
                    String[] msgComponents = latestMsg.getMsg().trim().split(msgSeparator);

                    if(msgComponents[0].equals("PUT"))
                    {
                        handlePut(msgComponents[1],msgComponents[2]);
                    }
                    else if(msgComponents[0].equals("GET"))
                    {
                        handleGet(msgComponents[1]);
                    }
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null)
				{
                    logger.info("KVClient <" + clientSocket.getInetAddress().getHostAddress() + ":" +
                            clientSocket.getPort() + "> Disconnected!");
					input.close();
					output.close();
					clientSocket.close();

				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	public void handlePut(String key, String value)
    {
        String putResponse;
        if(!value.equals("null") && !value.isEmpty() && !value.equals(""))
        {
            //Store KV-pair to disk
            try
            {
                storeKVPair(key, value);

                if(this.kvCache != null)//cache this kv-pair
                {
                    this.kvCache.insertInCache(key, value);
                    logger.info("<" + key + "," + value + ">" + " cached at KVServer" +
                            "<" + clientSocket.getInetAddress().getHostAddress()
                            + ":" + clientSocket.getLocalPort() + "> ");
                }
                putResponse = "PUT_SUCCESS";
                logger.info("PUT SUCCESSFUL: <key,value>: " + "<" + key + "," + value + ">");

            } catch (NoSuchAlgorithmException noAlgoEx) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "unable to hash key" + key);
                putResponse = "PUT_ERROR";

            } catch (IOException ex) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "UNABLE to STORE KV-pair to disk");
                putResponse = "PUT_ERROR";
            }
        }
        else
        {
            //Delete KV-pair from disk
            try {
                if(deleteFile(key))
                {
                    putResponse = "DELETE_SUCCESS";
                    logger.info("DELETE SUCCESSFUL: <key>: " + "<" + key + ">");
                }
                else
                {
                    putResponse = "DELETE_ERROR";
                    logger.error("DELETE ERROR: <key>: " + "<" + key + "> DOES NOT exist");
                }

                if(this.kvCache != null && this.kvCache.deleteFromCache(key))//evict this kv-pair from cache
                {
                    logger.info("<" + key + "," + value + ">" + " evicted from cached at KVServer" +
                            "<" + clientSocket.getInetAddress().getHostAddress()
                            + ":" + clientSocket.getLocalPort() + "> ");
                }
            } catch (NoSuchAlgorithmException noAlgoEx) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "unable to hash key" + key);
                putResponse = "DELETE_ERROR";

            } catch (IOException ex1) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "UNABLE to DELETE KV-pair from disk");
                putResponse = "DELETE_ERROR";
            }
        }

        //Send response message to KVClient
        try  {
            sendMessage(new TextMessage(putResponse));

        } catch (Exception ex) {
            //Design decision: KVServer may NOT be able to send PUT response, but KV-pair will be committed to disk
            logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + "UNABLE to SEND PUT response");
        }
    }

    public void storeKVPair(String key, String value) throws IOException, NoSuchAlgorithmException
    {
        String kvPairInJSON = key + ";" + value;

        String filePath = this.kvDirPath.getFileName() + File.separator + md5.HashInStr(key);
        Files.write(Paths.get(filePath),
                                kvPairInJSON.getBytes("utf-8"));

        logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                + ":" + clientSocket.getLocalPort() + ">\t" + "STORED: <" + key + "," + value + ">");
    }
    public boolean deleteFile(String key) throws IOException, NoSuchAlgorithmException
    {
        //the <value> sent by the KVClient will be "null"
        String filePath = this.kvDirPath.getFileName() + File.separator + md5.HashInStr(key);

        if(Files.deleteIfExists(Paths.get(filePath)))
        {
            logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + "DELETED: <" + key + ">");
            return true;
        }
        else
        {
            logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + ": " + key + " DOES NOT exist");
            return false;
        }
    }

    public void handleGet(String key)
    {
        String value;
        try
        {
            //check the cache to get kv-pair and send it to KVClient
            //Design Decision: no lock when reading a file for performance reasons
            if(this.kvCache == null)
            {
                value = getValueFromFile(key);
            }
            else if((value = this.kvCache.checkCache(key)) == null)
            {
                logger.info("Cache Miss!! <" + key + ">" + " at KVServer" +
                        "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> ");

                value = getValueFromFile(key);
            }
            else
            {
                logger.info("Cache Hit!! <" + key + ">" + " at KVServer" +
                        "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> ");
            }

            //Send <value> if it exists. Otherwise, send error to KVClient
            sendMessage(new TextMessage((value != null) ? value : "GET_ERROR"));

        }
        catch (Exception ex)
        {
            logger.error("GET FAILED: key: " + key );
        }
    }

    public String getValueFromFile(String key) throws IOException, NoSuchAlgorithmException
    {
        String filePath = this.kvDirPath.getFileName().toString() + File.separator + md5.HashInStr(key);
        if(Files.exists(Paths.get(filePath)))
        {
            byte[] data = Files.readAllBytes(Paths.get(filePath));
            String kvPairFormat = new String(data, "UTF-8");

            return kvPairFormat.split(";")[1];//get value object
        }
        else
        {
            logger.error("Error! KVServer " + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + "key: " + key + " DOES NOT EXIST");
            return null;
        }
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

//		Check if stream is closed (read returns -1)
		if (read == -1)
        {
            //terminate current request thread if KVClient sends disconnect
            isOpen = false;
		}

		while(/*read != 10  && */read != 13 && read !=-1 && reading) {/* CR, LF, error */
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
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
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

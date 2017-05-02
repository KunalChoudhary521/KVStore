package app_kvServer;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReentrantLock;

import cache.KVCache;
import common.md5;

import org.apache.log4j.*;
import server.TextMessage;

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
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private String kvDirPath;
	private KVCache kvCache;
	private static ReentrantLock fileLock = new ReentrantLock();//1 lock for all instances of ClientConnection
	
	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, String kvDir, KVCache cache)
    {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.kvDirPath = kvDir;
        this.kvCache = cache;
		try
        {
            fileLock.lock();
            createDirectory();
            fileLock.unlock();
        }
        catch (Exception ex)
        {
            logger.info("Directory /" + this.kvDirPath + " for KVServer <" +
                        clientSocket.getInetAddress().getHostAddress() + ":" +
                        clientSocket.getLocalPort() + "> NOT CREATED!!");
            this.isOpen = false;
            fileLock.unlock();
        }
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

                    if(latestMsg.getMsg().contains("PUT"))
                    {
                        handlePut(latestMsg);
                    }
                    else if(latestMsg.getMsg().contains("GET"))
                    {
                        handleGet(latestMsg);
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

	public void handlePut(TextMessage putMsg)
    {
        String[] parts = putMsg.getMsg().split(",");

        if(parts.length != 3)
        {
            logger.error("Error! PUT FAILED: key or value " +
                    "MUST NOT start with a comma (,)");
            return;
        }

        String key = parts[1];
        String value = parts[2].trim();//to remove \n\r after the <value>

        String putResponse;
        if(!value.equals("null") && !value.isEmpty() && !value.equals(""))
        {
            //Store KV-pair to disk
            try
            {
                fileLock.lock();
                storeKVPair(key, value);
                fileLock.unlock();

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
                        + ":" + clientSocket.getLocalPort() + "> " + "key" + key + " not hashed");
                putResponse = "PUT_ERROR";
                fileLock.unlock();

            } catch (IOException ex) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "UNABLE to STORE KV-pair to disk");
                putResponse = "PUT_ERROR";
                fileLock.unlock();
            }
        }
        else
        {
            //Delete KV-pair from disk
            try {
                fileLock.lock();
                deleteFile(key);
                fileLock.unlock();

                if(this.kvCache != null)//evict this kv-pair from cache
                {
                    this.kvCache.deleteFromCache(key);
                    logger.info("<" + key + "," + value + ">" + " evicted from cached at KVServer" +
                            "<" + clientSocket.getInetAddress().getHostAddress()
                            + ":" + clientSocket.getLocalPort() + "> ");
                }
                putResponse = "DELETE_SUCCESS";
                logger.info("DELETE SUCCESSFUL: <key>: " + "<" + key + ">");

            } catch (NoSuchAlgorithmException noAlgoEx) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "key" + key + " not hashed");
                putResponse = "DELETE_ERROR";
                fileLock.unlock();

            } catch (IOException ex1) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "UNABLE to DELETE KV-pair from disk");
                putResponse = "DELETE_ERROR";
                fileLock.unlock();
            }
        }

        //Send response message to KVClient
        try  {
            sendMessage(new TextMessage(putResponse));

        } catch (Exception ex) {
            //Design decision: KVServer may NOT be able to send PUT response, but KV-pair will be committed to disk
            logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + "UNABLE to SEND PUT response");
            return;
        }
    }

    public void createDirectory() throws IOException
    {
        if (Files.notExists(Paths.get(this.kvDirPath)))
        {
            Files.createDirectory(Paths.get(this.kvDirPath));

        logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                + ":" + clientSocket.getLocalPort() + "> " + "New Directory: "
                + System.getProperty("user.dir") + File.separator + this.kvDirPath);
        }
    }
    public void storeKVPair(String key, String value) throws IOException, NoSuchAlgorithmException
    {
        String kvPairInJSON = "{ \"key\":\"" + key + "\", \"value\":\"" + value + "\" }";

        String filePath = this.kvDirPath + File.separator + md5.HashInStr(key);
        Files.write(Paths.get(filePath),
                                kvPairInJSON.getBytes("utf-8"));

        logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                + ":" + clientSocket.getLocalPort() + ">\t" + "STORED: <" + key + "," + value + ">");
    }
    public void deleteFile(String key) throws IOException, NoSuchAlgorithmException
    {
        //the <value> sent by the KVClient will be "null"
        String filePath = this.kvDirPath + File.separator + md5.HashInStr(key);

        if(Files.deleteIfExists(Paths.get(filePath)))
        {
            logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + "DELETED: <" + key + ">");
        }
        else
        {
            logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> " + ": " + key + " DOES NOT exist");
        }
    }

    public void handleGet(TextMessage getMsg)
    {
        String[] parts = getMsg.getMsg().split(",");

        if(parts.length != 2)
        {
            logger.error("Error! GET FAILED: key MUST NOT start with a comma (,)");
            return;
        }

        String key = parts[1].trim();//to remove \n\r after the <value>
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

            sendMessage(new TextMessage(value));
        }
        catch (Exception ex)
        {
            logger.error("GET FAILED: key: " + key );
        }
    }

    public String getValueFromFile(String key) throws IOException, NoSuchAlgorithmException
    {
        String filePath = this.kvDirPath + File.separator + md5.HashInStr(key);
        if(Files.exists(Paths.get(filePath)))
        {
            byte[] data = Files.readAllBytes(Paths.get(filePath));
            String kvPairInJSON = new String(data, "UTF-8");

            String valueField = kvPairInJSON.split(",")[1];//get value object

            String rawValue = valueField.split(":")[1];
            String value = rawValue.substring(1,rawValue.length()-3);//ignore characters <" }>

            return value;
        }
        else
        {
            sendMessage(new TextMessage("GET_ERROR"));
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

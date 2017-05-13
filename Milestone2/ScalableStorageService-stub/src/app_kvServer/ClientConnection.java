package app_kvServer;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import client.KVStore;
import common.Metadata;
import common.md5;

import org.apache.log4j.*;
import common.TextMessage;

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
	private static ReentrantLock fileLock = new ReentrantLock();
    private static boolean isWriteLocked;
    private static boolean isReadLocked;
    private static ServerInfo kvServerInfo;
    private static ServerSocket kvServerSock;
	
	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, ServerInfo sInfo, ServerSocket serverSocket)
    {
        this.isOpen = true;
        this.clientSocket = clientSocket;
		kvServerInfo = sInfo;
        kvServerSock = serverSocket;
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
					//TODO: declare a static var. msgSeparator = "," in here & ECS class
                    String[] msgComponents = latestMsg.getMsg().split(",");

                    if(msgComponents[0].equals("PUT"))
                    {
                        handlePut(latestMsg);
                    }
                    else if(msgComponents[0].equals("GET"))
                    {
                        handleGet(latestMsg);
                    }
                    else if(msgComponents[0].equals("KVPAIR"))
                    {
                        for(int i = 1; i < msgComponents.length; i++)
                        {
                            String[] kvPair = msgComponents[i].split(";");//key;value
                            storeKVPair(kvPair[0], kvPair[1]);
                        }
                    }
                    else if(msgComponents[0].equals("ECS"))
                    {
                        //KVServer is stopped when it is being added to the ring
                        if(msgComponents[1].equals("START_SERVER")) {
                            isReadLocked = false;
                            isWriteLocked = false;
                            sendMessage(new TextMessage("SERVER_STARTED"));
                        } if(msgComponents[1].equals("STOP_SERVER")) {
                            isReadLocked = true;
                            isWriteLocked = true;
                            sendMessage(new TextMessage("SERVER_STOPPED"));
                        } else if(msgComponents[1].equals("LOCKWRITE")) {
                            //KVServer's write is locked ONLY while it is being removed
                            isWriteLocked = true;
                            sendMessage(new TextMessage("SERVER_WRITE_LOCK"));
                        } else if(msgComponents[1].equals("UNLOCKWRITE")) {
                            isWriteLocked = false;
                            sendMessage(new TextMessage("SERVER_WRITE_UNLOCK"));
                        } else if(msgComponents[1].equals("METADATA")) {
                            String[] metaDatalines = new String[msgComponents.length - 2];
                            System.arraycopy(msgComponents,2,metaDatalines,
                                        0,metaDatalines.length);
                            storeMetadata(metaDatalines);
                            setHashRange(metaDatalines);
                            sendMessage(new TextMessage("RECEIVED_METADATA"));
                        } else if(msgComponents[1].equals("TRANSFER")) {
                            sendKVPairs(msgComponents[2]);
                            sendMessage(new TextMessage("TRANSFER_DONE"));
                        } else if(msgComponents[1].equals("SHUTDOWN")) {
                            isWriteLocked = true;
                            isReadLocked = true;
                            isOpen = false;
                            kvServerInfo.setRunning(false);
                            kvServerSock.close();
                            sendMessage(new TextMessage("SHUTDOWN_SUCCESS"));
                        }
                    }
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (Exception ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;//ClientConnection Thread closes

                    //general error message to ECS if KVServer fails to perform a task given by ECS
                    sendMessage(new TextMessage("ERROR"));
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null)
				{
				    //TODO: log in (server.log) mentions KVClient disconnected even though ECS disconnected
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
        try {
            if (isWriteLocked) {
                sendMessage(new TextMessage("SERVER_WRITE_LOCK"));
                return;
            }
        } catch (IOException ex) {
            logger.error("Error! Write Locked! at KVServer" +
                    "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> ");
            return;
        }

        String[] parts = putMsg.getMsg().split(",");
        String key = parts[1];
        String value = parts[2].trim();//to remove \n\r after the <value>

        //Check if this is the server responsible for the (key,value)
        try {
            String keyHash = md5.HashInBI(key).toString(16);

            //handle the case where KVServer has no metadata file
            if(kvServerInfo.getStartHash() == null || kvServerInfo.getEndHash() == null) {
                logger.error("Error! start or end hash range not set");
                sendMessage(new TextMessage("PUT_ERROR"));
                return;
            } else if(!isKeyInRange(keyHash,kvServerInfo.getStartHash(),kvServerInfo.getEndHash())) {
                sendMessage(new TextMessage("SERVER_NOT_RESPONSIBLE"));

                TextMessage requestForMetadata = receiveMessage();//part2 of this protocol
                if(requestForMetadata.getMsg().equals("SEND_METADATA")) {
                    String metaDataMsg = createMetadataMsg();
                    sendMessage(new TextMessage(metaDataMsg));
                }

                return;
            }
        } catch (IOException ex) {
            logger.error("Error! Unable to complete SERVER_NOT_RESPONSIBLE protocol for " + key);
            return;
        } catch (NoSuchAlgorithmException ex) {
            logger.error("Unable to hash key using MD5");
            return;
        }

        String putResponse;
        if(!value.equals("null") && !value.isEmpty() && !value.equals(""))
        {
            //Store KV-pair to disk
            try
            {
                fileLock.lock();
                storeKVPair(key, value);
                fileLock.unlock();

                if(kvServerInfo.getCache() != null)//cache this kv-pair
                {
                    kvServerInfo.getCache().insertInCache(key, value);
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
                if(deleteFile(key))
                {
                    putResponse = "DELETE_SUCCESS";
                    logger.info("DELETE SUCCESSFUL: <key>: " + "<" + key + ">");
                }
                else
                {
                    putResponse = "DELETE_ERROR";
                    logger.info("DELETE ERROR: <key>: " + "<" + key + "> DOES NOT exist");
                }
                fileLock.unlock();

                if(kvServerInfo.getCache() != null)//evict this kv-pair from cache
                {
                    kvServerInfo.getCache().deleteFromCache(key);
                    logger.info("<" + key + "," + value + ">" + " evicted from cached at KVServer" +
                            "<" + clientSocket.getInetAddress().getHostAddress()
                            + ":" + clientSocket.getLocalPort() + "> ");
                }
            } catch (NoSuchAlgorithmException noAlgoEx) {
                logger.error("Error! KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getLocalPort() + "> " + "unable to hash key" + key);
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
        }
    }

    public void storeKVPair(String key, String value) throws IOException, NoSuchAlgorithmException
    {
        String kvPairFormat = key + ";" + value;

        String filePath = kvServerInfo.getKvDirPath().getFileName().toString() +
                            File.separator + md5.HashInStr(key);

        Files.write(Paths.get(filePath), kvPairFormat.getBytes("utf-8"));

        logger.info("KVServer" + "<" + clientSocket.getInetAddress().getHostAddress()
                + ":" + clientSocket.getLocalPort() + ">\t" + "STORED: <" + key + "," + value + ">");
    }

    public boolean deleteFile(String key) throws IOException, NoSuchAlgorithmException
    {
        //the <value> sent by the KVClient will be "null"
        String filePath = kvServerInfo.getKvDirPath().getFileName().toString() +
                        File.separator + md5.HashInStr(key);

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

    /**
     * Assumption: key is hashed using md5 algorithm
     *
     * @param keyHash   hash of a key to compare with start & end hash
     * @param sHash   start hash (may or may not be of this KVServer)
     * @param eHash   end hash (may or may not be of this KVServer)
     * @return      true if keyHash is within range (inclusive) and false otherwise
     */
    public boolean isKeyInRange(String keyHash, String sHash, String eHash)
    {
        if(sHash.compareTo(eHash) <= 0)
        {
            if (keyHash.compareTo(sHash) >= 0 && keyHash.compareTo(eHash) <= 0)
            {
                return true;
            }
        }
        else //wrap-around case
        {
            String minHash = new String(new char[32]).replace("\0", "0");//000...00
            String maxHash = new String(new char[32]).replace("\0", "f");//fff...fff

            if ((keyHash.compareTo(sHash) >= 0 && keyHash.compareTo(maxHash) <= 0) ||
                    (keyHash.compareTo(minHash) >= 0 && keyHash.compareTo(eHash) <= 0))
            {
                return true;
            }
        }
        return false;
    }

    public String createMetadataMsg() throws IOException
    {
        StringBuilder createMsg = new StringBuilder();
        if(Files.exists(kvServerInfo.getmDataFile()))
        {
            ArrayList<String> metaData = new ArrayList<>(Files.readAllLines(kvServerInfo.getmDataFile(),
                    StandardCharsets.UTF_8));

            for(String line : metaData)
            {
                createMsg.append(line + ",");
            }
        }
        else
        {
            createMsg.append("NO_METADATA");
        }

        return createMsg.toString();
    }

    public void handleGet(TextMessage getMsg)
    {
        try {
            if (isReadLocked) {
                sendMessage(new TextMessage("SERVER_STOPPED"));
                return;
            }
        } catch (IOException ex) {
            logger.error("Error! KVServer" +
                    "<" + clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + "> STOPPED. No get " +
                    "or put allowed");
            return;
        }

        String[] parts = getMsg.getMsg().split(",");

        String key = parts[1].trim();//to remove \n\r after the <value>

        //Check if this is the server responsible for the (key,value)
        try {
            String keyHash = md5.HashInBI(key).toString(16);
            if(kvServerInfo.getStartHash() == null || kvServerInfo.getEndHash() == null) {
                logger.error("Error! start or end hash range not set");
                sendMessage(new TextMessage("GET_ERROR"));
                return;
            } else if(!isKeyInRange(keyHash,kvServerInfo.getStartHash(),kvServerInfo.getEndHash())) {
                sendMessage(new TextMessage("SERVER_NOT_RESPONSIBLE"));

                TextMessage requestForMetadata = receiveMessage();//part2 of this protocol
                if(requestForMetadata.getMsg().equals("SEND_METADATA")) {
                    String metaDataMsg = createMetadataMsg();
                    sendMessage(new TextMessage(metaDataMsg));
                }

                return;
            }
        } catch (IOException ex) {
            logger.error("Error! Unable to complete SERVER_NOT_RESPONSIBLE protocol for " + key);
            return;
        } catch (NoSuchAlgorithmException ex) {
            logger.error("Unable to hash key using MD5");
            return;
        }

        String value;
        try
        {
            //check the cache to get kv-pair and send it to KVClient
            //Design Decision: no lock when reading a file for performance reasons
            if(kvServerInfo.getCache() == null)
            {
                value = getValueFromFile(key);
            }
            else if((value = kvServerInfo.getCache().checkCache(key)) == null)
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
        String filePath = kvServerInfo.getKvDirPath().getFileName().toString() +
                        File.separator + md5.HashInStr(key);
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

    public void storeMetadata(String[] lines)
    {
        try {
            ArrayList<String> fileContents = new ArrayList<>(Arrays.asList(lines));
            Files.write(kvServerInfo.getmDataFile(), fileContents, StandardCharsets.UTF_8);

        } catch (IOException ex) {
            logger.error("Error! Unable to store meta data KVServer<" +
                        clientSocket.getInetAddress().getHostAddress()
                    + ":" + clientSocket.getLocalPort() + ">");
        }
    }

    public void setHashRange(String[] lines)
    {
        for(String line : lines)
        {
            String[] components = line.split(";");
            if(components[0].equals(clientSocket.getInetAddress().getHostAddress())
                    && Integer.parseInt(components[1]) == clientSocket.getLocalPort())
            {
                kvServerInfo.setStartHash(components[2]);
                kvServerInfo.setEndHash(components[3]);
                logger.error("startHash & endHash set");
                break;
            }
        }
    }

    public void sendKVPairs(String dstServerInfo) throws IOException
    {
        String[] info = dstServerInfo.split(";");
        Metadata dstServer = new Metadata(info[0], Integer.parseInt(info[1]),info[2], info[3]);

        //get a list of all the KVPair files (ignore metadata file)
        String keyHash;
        StringBuilder fileContent = new StringBuilder();
        DirectoryStream<Path> stream = Files.newDirectoryStream(kvServerInfo.getKvDirPath());
        for(Path path : stream) {
            if(path.toString().contains("metadata.txt"))//ignore metadata file
            {
                logger.info("Ignoring metadata file: " + path.toString());
                continue;
            }
            //if keyHash (file name) is in dstServer's range, read the file contents and sendMessage
            keyHash = path.toString();
            if(isKeyInRange(keyHash,dstServer.startHash,dstServer.endHash))
            {
                byte[] data = Files.readAllBytes(Paths.get(keyHash));
                String kvPair = new String(data, "UTF-8");
                fileContent.append(kvPair + ",");
                Files.deleteIfExists(path);//delete KVPairs that were sent from srcServer
            }
        }

        KVStore tempClient = new KVStore(dstServer.address,dstServer.port);
        tempClient.connect();
        tempClient.sendMessage(new TextMessage("KVPAIR," + fileContent.toString()));
        tempClient.disconnect();
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
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }
            //bufferBytes[index] = read;index++;
			
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
				+ clientSocket.getPort() + ">:");
				//+ msg.getMsg().trim() + "'");
		return msg;
    }
}

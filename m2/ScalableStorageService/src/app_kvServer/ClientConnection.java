package app_kvServer;

import app_kvEcs.md5;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.TreeMap;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger  logger = Logger.getRootLogger();


	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer server;
	private FileStoreHelper fileStoreHelper;
	private boolean log;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server, String KVFileName, boolean log) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
		this.log = log;
		this.fileStoreHelper = new FileStoreHelper(KVFileName, this.log);
		//String[] files = fileStoreHelper.getOutOfRangeFiles();
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while(isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();
					logger.info("client sent" + latestMsg.getMsg());
					if (latestMsg.getMsg().trim().charAt(0)== 'P') {
						if (this.server.isStarted) {
							handle_put();
						} else {
							byte[] message = {'S', 'T', 0};
							sendMessage(message, 3);
						}
					} else if (latestMsg.getMsg().trim().charAt(0)== 'G') {
						if (this.server.isStarted) {
							handle_get();
						} else {
							byte[] message = {'S', 'T', 0};
							sendMessage(message, 3);
						}
					} else if (latestMsg.getMsg().trim().contains("ECS")) {
						handle_ecs(latestMsg.getMsg());
					}
					else if(latestMsg.getMsg().trim().contains("D")){
						isOpen = false;//client disconnect
					} else if (latestMsg.getMsg().trim().contains("KV-MOVE")){
						 // the function that handles all of the data sent by a different KVServer
                        String[] msgComponents = (latestMsg.getMsg()).split("-");
                        String[] kvPairRecv = msgComponents[2].split(",");//need to parse XML to hash key
                        String[] xmlKV;
                        BufferedWriter kvOut;
                        String fileName = null;
                        try
                        {
                            for (int i = 0; i < kvPairRecv.length; i++)
                            {
                                xmlKV = kvPairRecv[i].split("\"");
                                //System.out.println("key: " + xmlKV[1]);
                                fileName = this.server.getPort() + "/" + md5.HashS(xmlKV[1]);
                                kvOut = new BufferedWriter(new FileWriter(fileName));
                                kvOut.write(kvPairRecv[i],0,kvPairRecv[i].length());
                                kvOut.flush();

                                kvOut.close();
                                logger.info("Receiver KVServer has received KV-Pairs");
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.error("KV-MOVE:: Error will writing kvpairs to file");
                        }

					}
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					if(log) {
						logger.error("Error! Connection lost!");
					}
					isOpen = false;
				}catch (Exception ex){
					logger.error(ex.getMessage());
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void handle_ecs(String msg) {
		if(msg.contains("ECS-LOCKWRITE")){ //can only read
			this.server.isReadOnly = true;
			logger.info("ECS message: lockwrite");
		}else if(msg.contains("ECS-UNLOCKWRITE")){ //can write now
			this.server.isReadOnly = false;
			logger.info("ECS message: unlockwrite");
		} else if(msg.contains("ECS-SHUTDOWN")){ //terminate
			this.server.Shutdown(true);
			this.server.closeAllSockets();
			isOpen = false;
			logger.info("ECS message: shutdown");
		} else if(msg.contains("ECS-START")){ //start
            //server should not accept any puts or gets from KVClient (m2-spec)
			this.server.isStarted = true;
			logger.info("ECS message: start");
		} else if(msg.contains("ECS-STOP")){ //stop
			this.server.isStarted = false;
			logger.info("ECS message: stop");
		} else if(msg.contains("ECS-MOVE-KV")) { //move data that falls in the range to a new server

            String[] params = msg.split("-");

            // need to be told by the ECS which server you must send data to (get from message)
            // get the target server host and port (get from message)
            String dstServerIP = params[3];
            String dstServerPort = params[4];

            // get the hash range from the message (get from message)
            String dstServerStart = params[5];
            String dstServerEnd = params[6];

            // pull all the files on the filesystem (fileStoreHelper)
            // filter them based on the hash range given to you by the ECS
            ArrayList<File> filesToSend = this.fileStoreHelper.getInRangeFiles(dstServerStart, dstServerEnd);
            if (filesToSend.isEmpty()) {
                logger.error("ECS-MOVE-KV :: No files to move");
                //return;//return here?
            }

            // open each file in the filtered list, and create a map of key value pairs
            StringBuilder kvPairBuilder = new StringBuilder();
            String kvPairContents = null;
            kvPairBuilder.append("KV-MOVE-");

            BufferedReader currentFile;
            try {
                for (int i = 0; i < filesToSend.size(); i++)
                {
                    currentFile = new BufferedReader(new FileReader(filesToSend.get(i)));
                    kvPairBuilder.append(currentFile.readLine());
                    if(i < filesToSend.size() - 1)
                    {
                        kvPairBuilder.append(",");//don't put comma after the last entry
                    }

                }
                kvPairContents = kvPairBuilder.toString();


            }
            catch (Exception ex) {
                logger.error("ECS-MOVE-KV :: Could not open file to send from Server");
            }

            // based on a server-server protocol, create a byte array of all the key value pairs that fall
            // inside the hash range
            // if you turn into a string, turn it into a byte array
            byte[] kvPairBArray = new byte[kvPairContents.length() + 1];//+1 for stream termination
            System.arraycopy(kvPairContents.getBytes(), 0, kvPairBArray, 0, kvPairContents.length());

            // implement a logic that each KVServer should be able to handle server-server protocol
            logger.info("ECS-KV-MOVE:: KVPair message is ready to be sent!");
            try
            {
                // create a socket, and establish a connection with the target server
                Socket kvSenderSock = new Socket(dstServerIP,Integer.parseInt(dstServerPort));

                //Thread.sleep(2* 1000);//need a better way to make sure receiver is ready to receive

                // write the byte array to the output stream "KV-MOVE-<>,<>,...,<>0"
                OutputStream writeToSock = kvSenderSock.getOutputStream();
                InputStream input = kvSenderSock.getInputStream();

                writeToSock.write(kvPairBArray,0,kvPairBArray.length);
                writeToSock.flush();

                kvSenderSock.close();
                //Do I need to send disconnect message to kill the thread on receiver side?
            }
            catch (Exception ex)
            {
                logger.error("ECS-MOVE-KV :: Failed to connect with KV pair receiver");
            }


            // send an ACK to ECS saying "DONE-TRANSFER" after moving
            String doneMsg = "DONE-TRANSFER";
            byte[] doneACK = new byte[doneMsg.length() + 1];
            System.arraycopy(doneMsg.getBytes(),0,doneACK,0,doneMsg.length());
            try
            {
                sendMessage("FIN".getBytes(),3);//ECS gets out of sendViaTCP of ECS-MOVE-KV
                this.sendMessage(doneACK,doneACK.length);
            }
            catch (Exception ex)
            {
                logger.error("ECS-MOVE-KV:: Could not send ACK to ECS.");
            }

            // delete the all of the files in the filtered list
			/*
            for(int i =0; i < filesToSend.size(); i++)
            {
                //Uncomment after moveData works correctly
                if(!filesToSend.get(i).delete())
                {
                    logger.error("ECS-MOVE-KV:: Could not delete: " + filesToSend.get(i).getName());
                }

            }
            String serverDirPath = System.getProperty("user.dir") + this.server.getPort() + "/";
            File myMetadataFile = new File(serverDirPath + "metadata");

            File serverDir = new File(serverDirPath);
            if(serverDir.isDirectory())
            {
                if ((serverDir.length() == 1) && (myMetadataFile.exists()))
                {
                    //remove server's metadata file & directory
                    //myMetadataFile.delete();
                    if(serverDir.delete())//deletes metadata file too
                    {
                        logger.info("KV-MOVE:: Successfully deleted " + this.server.getPort()
                                    + "'s directory");
                    }
                }
            }
			*/

            logger.info("ECS message: move\n");

		} else if(msg.contains("ECS-METADATA")){ // must replace current metadata with the metadata being given
			logger.info("ECS message: metadata");
			int i = "ECS-METADATA-".length();
			String host = "", port = "", startHash_g = "", startHash_p = "", endHash="";
			TreeMap<BigInteger,Metadata> newMetadata = new TreeMap<>();
			while(msg.charAt(i) != '\n'){
				// get the host
				while(msg.charAt(i) != ','){
					host += msg.charAt(i);
					i++;
				}
				i++; //move past comma
				// get the host
				while(msg.charAt(i) != ','){
					port += msg.charAt(i);
					i++;
				}
				i++; //move past comma
				// get the host
				while(msg.charAt(i) != ','){
					startHash_g += msg.charAt(i);
					i++;
				}
				i++; //move past comma

				while(msg.charAt(i) != '-' && msg.charAt(i) != '\n'){
					startHash_p += msg.charAt(i);
					i++;
				}
				i++;
				while(msg.charAt(i) != '-' && msg.charAt(i) != '\n'){
					endHash += msg.charAt(i);
					i++;
				}
				if(msg.charAt(i) == '-') {
					i++;
				}
				newMetadata.put(new BigInteger(endHash),new Metadata(host,port,startHash_g,startHash_p,endHash));
				host = "";
				port = "";
				startHash_g = "";
				startHash_p = "";
				endHash = "";
			}
			takeNewMetadata(newMetadata);
		} else if(msg.contains("ECS-DISCONNECT")){
			this.isOpen = false;
		}
		if(this.isOpen) {
			sendECSAck();
		}
	}

	private void takeNewMetadata(TreeMap<BigInteger,Metadata> newMetadata) {
		this.server.takeNewMetadata(newMetadata);
	}

	public void sendECSAck(){
		byte[] ack = "FIN".getBytes();
		try {
			this.sendMessage(ack, ack.length);
		}
		catch (Exception ex){
			logger.info(ex);
		}
	}

	/**
	 * All the parsing logic for handling a get request
	 * Also calls the cache and FileStore instances to obtain kvp
	 * Builds back a message and sends it back to the client
	 * */
	public void handle_get() throws Exception, IOException{
		String key = receiveMessage().getMsg().trim();
		if(log) {
			logger.info("Client tried to get key: '" + key + "'");
		}
		try{
			if (key.length()>20){
				throw new Exception("Client sent too large a key, key = '"+key+"', size = "+key.length());
			}
			int length = 0;

			//check if this node is responsible for the key
			String hash = md5.HashS(key);
			if(log){
				logger.info(hash);
			}

			if(!server.isResponsible(hash, "get"))
			{
				String message = "I!";
				message+=server.getMetadata();
				byte[] barray = new byte[message.length()];
				for(int i = 0; i <message.length(); i++){
					if(message.charAt(i) == '!'){
						barray[i] = 0;
					} else {
						barray[i] = (byte) message.charAt(i);
					}
				}
				sendMessage(barray, barray.length);
				return;
			}

			//check if we have the key in our file
			logger.debug("checking cache");
			String payload = this.server.findInCache(key,log);
			logger.debug("cache returned"+payload);

			int got_key = 0;

			if(payload == null)
			{
				// was not found in cache find in file
				got_key = 0;
				logger.debug("not in cache, checking file");
				String result = fileStoreHelper.FindFromFile(key);
				logger.info("file returned"+result);
				if(result != null){
					// PUT in the cache if there is space
					logger.debug("putting in cache");
					this.server.getKvcache().insertInCache(key,result);//is result Value?
					logger.info("put in cache");

					got_key = 1;
					payload = result;
					length = payload.length();
				}
			} else {
				// was found in cache
				got_key = 1;
				length = payload.length();
			}


			String length_str = Integer.toString(length);


			if (got_key == 1){
				//creating response
				int kl = key.length();
				int ll = length_str.length();
				byte[] message = new byte[5+kl+ll+length];
				message[0]=(byte) 'S';
				message[1]=(byte) 0;
				for (int i = 0; i < kl; i++){
					message[2+i] =(byte) key.charAt(i);
				}
				message[2+kl]= (byte) 0;
				for (int i = 0; i <ll; i++){
					message[3+kl+i]=(byte) length_str.charAt(i);
				}
				message[3+kl+ll] = (byte) 0;
				for (int i = 0; i < length; i ++){
					message[4+kl+ll+i] = (byte) payload.charAt(i);
				}
				message[4+kl+ll+length]=(byte)0;
				logger.info("sending response");
				//respond
				this.sendMessage(message, 5+kl+ll+length);
			}
			else{
				logger.warn("client sent non-existent key");
				byte[] message = new byte[2];
				message[0]=(byte) 'F';
				message[1]=(byte) 0;
				this.sendMessage(message, 2);
				String msg = "Get, client sent non-existent key, key = '"+key+"'";
				if(log) {
					logger.info(msg);
				}
			}
		}
		catch(Exception ex) {
			logger.info(ex.getMessage());
		}
	}

	/**
	 * All the parsing logic for handling a put request
	 * Also calls the cache and FileStore instances to store the kvp
	 * Builds back a message and sends it back to the client
	 * */
	public void handle_put(){
		if(this.server.isReadOnly)
		{
			byte[] message = new byte[2];
			message[0] = 'W';
			message[1] = 0;
			try {
				sendMessage(message, 1);
			} catch(Exception ex){
				logger.info(ex);
			}
		} else {
			String [] client_msgs = new String[4];
			try{
				for (int i =0; i<2; i++){
					client_msgs[i] = this.receiveMessage().getMsg().trim();
				}
				if(log) {
					logger.info("Put, client wants to place " + client_msgs[1] + " bytes for key '" + client_msgs[0] + "'");
				}
				int kl = client_msgs[0].length();
				int ll = client_msgs[1].length();
				//validate key and payload lengths
				if (kl>20){
					byte [] message = new byte [2];
					message[0]=(byte) 'F';
					message[1] = (byte) 0;
					this.sendMessage(message, 2);
					throw new Exception("Put, client sent too long of a key, key = '"+client_msgs[0]+"', length = "+kl);
				}
				//check if this node is responsible for the key
				String hash = md5.HashS(client_msgs[0]);
				if(log){
					logger.info(hash);
				}

				if(!server.isResponsible(hash,"put"))
				{
					//sending client the latest copy of metadata of this server
					String message = "I!";
					message+=server.getMetadata();
					byte[] barray = new byte[message.length()];
					for(int i = 0; i <message.length(); i++){
						if(message.charAt(i) == '!'){
							barray[i] = 0;
						} else {
							barray[i] = (byte) message.charAt(i);
						}
					}
					sendMessage(barray, barray.length);
					return;
				}

				if (Integer.valueOf(client_msgs[1].trim())>(120*1024)){
					byte [] message = new byte [2];
					message[0]=(byte) 'F';
					message[1] = (byte) 0;
					this.sendMessage(message, 2);
					throw new Exception("Put, client sent too large a size, size = "+client_msgs[1]);
				}
				//GET the key from the file on disk populate below variables based on file
				logger.info("key and length passed validation");
				int got_key = 0;
				if ((this.server.findInCache(client_msgs[0],log)!=null) || fileStoreHelper.FindFromFile(client_msgs[0])!=null){
					got_key = 1;
				}
				logger.debug("got_key="+got_key);

				byte[] message = new byte[4+kl+ll];
				if (got_key == 1){
					message[0] = (byte) 'U';
				}
				else{
					message[0] = (byte) 'S';
				}
				message[1] = (byte) 0;
				for (int i = 0; i<kl; i++){
					message[2+i]=(byte) client_msgs[0].charAt(i);
				}
				message[2+kl]= (byte) 0;
				for (int i = 0; i < ll; i++){
					message[3+kl+i] = (byte) client_msgs[1].charAt(i);
				}
				message[3+kl+ll]=(byte) 0;

				this.sendMessage(message, 4+kl+ll);
				logger.info("acknowledgement sent, awaiting payload");
				for (int i = 2; i<4; i++){
					client_msgs[i] = this.receiveMessage().getMsg().trim();

				}

				if (client_msgs[2].contains("F")){
					throw new Exception("Put, client sent a failure signal");
				}
				if(client_msgs[3].length() != Integer.parseInt(client_msgs[1].trim())){
					throw new Exception("Put, client sent a payload of the incorrect size, expected "+client_msgs[1]+", got "+client_msgs[3].length());
				}

				int cacheSuccess =0; //change based on insertion results
				int fileSuccess = 0; //initially always a failure, have to set it to 1 for success

				if(got_key == 0 && !client_msgs[3].equals("null")) {
					// PUT

					this.server.addToCache(client_msgs[0],client_msgs[3]);
					cacheSuccess = 1;


					logger.debug("cached");
					FileStoreHelper.FileStoreStatusType result = fileStoreHelper.PutInFile(client_msgs[0], client_msgs[3]);
					if(result == FileStoreHelper.FileStoreStatusType.PUT_SUCCESS)
					{
						logger.info("put successfully");
						fileSuccess = 1;
					}
				} else {
					// UPDATE OR DELETE
					if(client_msgs[3].equals("null")){
						// DELETE
						logger.debug("deleting");
						FileStoreHelper.FileStoreStatusType result = fileStoreHelper.DeleteInFile(client_msgs[0]);

						if(result == FileStoreHelper.FileStoreStatusType.DELETE_SUCCESS){
							logger.debug("deleted");
							fileSuccess = 1;
							this.server.getKvcache().deleteFromCache(client_msgs[0]);

						}
					} else {
						// UPDATE
						logger.debug("updating");
						FileStoreHelper.FileStoreStatusType result = fileStoreHelper.UpsertInFile(client_msgs[0], client_msgs[3]);

						if(result == FileStoreHelper.FileStoreStatusType.UPSERT_SUCCESS){
							fileSuccess = 1;
							logger.debug("file updated");
							this.server.getKvcache().insertInCache(client_msgs[0], client_msgs[3]);
							logger.debug("cache updated");
						}
					}
				}


				///
				if (fileSuccess == 1){
				    this.server.update_outstanding_update("<entry key=\""+client_msgs[0]+"\">"+client_msgs[3]+"</entry>");
					byte [] ack = new byte[2];
					ack[0] = (byte) 'S';
					ack[1] = (byte) 0;
					this.sendMessage(ack, 2);
					throw new Exception("Put succeeded, key = '"+client_msgs[0]+"' payload = '"+client_msgs[3]+"'");
				}
				else{
					byte [] ack = new byte[2];
					ack[0] = (byte) 'F';
					ack[1] = (byte) 0;
					this.sendMessage(ack, 2);
					throw new Exception("Put failed, key = '"+client_msgs[0]+"' payload = '"+client_msgs[3]+"'");
				}
			}
			catch(Exception ex) {
				logger.info(ex.getMessage());

			}
		}
	}

	/**
	 * Mostly from the stub code. Sends a message to the client by writing in the stream
	 * */
    public void sendMessage(byte[] msg, int len) throws IOException {
		output.write(msg, 0, len);
		output.flush();
		String message = new String(msg,0,len);
		if(log) {
			logger.info("Send message:\t '" + message + "'");
		}
    }

	/**
	 * Mostly from the stub code. Receives and parses a message sent from a client
	 * Reads from the input stream
	 * */
    private TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 0 && reading) {/* carriage return */
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
			if((read > 0 && read < 127)) {
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
		if(log) {
			logger.info("Receive message:\t '" + msg.getMsg() + "'");
		}
		return msg;
    }
}


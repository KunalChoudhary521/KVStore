package app_kvServer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import java.io.*;//remove me later


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
		this.fileStoreHelper = new FileStoreHelper(KVFileName);
		this.log = log;
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
					if (latestMsg.getMsg().trim().contains("P")){
						handle_put();
						
					}
					else if (latestMsg.getMsg().trim().contains("G")){
						handle_get();
					}


				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					if(log) {
						logger.info("Error! Connection lost!");
					}
					isOpen = false;
				}catch (Exception ex){
					logger.info(ex.getMessage());
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
			//check if we have the key in our file
			String payload = this.server.findInCache(key,log);


			int got_key = 0;

			if(payload == null)
            {
                // was not found in cache find in file
                got_key = 0;
                String result = fileStoreHelper.FindFromFile(key,log);
                if(result != null){
                	// PUT in the cache if there is space
					this.server.getKvcache().insertInCache(key,result);//is result Value?

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
			//
			
			if (got_key == 1){
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
				this.sendMessage(message, 5+kl+ll+length);
			}
			else{
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
	public void handle_put(){
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

			if (kl>20){
				byte [] message = new byte [2];
				message[0]=(byte) 'F';
				message[1] = (byte) 0;
				this.sendMessage(message, 2);
				throw new Exception("Put, client sent too long of a key, key = '"+client_msgs[0]+"', length = "+kl);
			}
			if (Integer.valueOf(client_msgs[1].trim())>(120*1024)){
				byte [] message = new byte [2];
				message[0]=(byte) 'F';
				message[1] = (byte) 0;
				this.sendMessage(message, 2);
				throw new Exception("Put, client sent too large a size, size = "+client_msgs[1]);
			}
			//GET the key from the file on disk populate below variables based on file

			int got_key = 0;
			if ((this.server.findInCache(client_msgs[0],log)!=null) || fileStoreHelper.FindFromFile(client_msgs[0],log)!=null){
				got_key = 1;
			}

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
			for (int i = 2; i<4; i++){
				client_msgs[i] = this.receiveMessage().getMsg().trim();

			}

			if (client_msgs[2].contains("F")){
				throw new Exception("Put, client sent a failure signal");
			}
			if(client_msgs[3].length() != Integer.parseInt(client_msgs[1].trim())){
				throw new Exception("Put, client sent a payload of the incorrect size, expected "+client_msgs[1]+", got "+client_msgs[3].length());
			}
			///overwrite the payload or whatever other file stuff her


			int cacheSuccess =0; //change based on insertion results
			int fileSuccess = 0; //initially always a failure, have to set it to 1 for success

			if(got_key == 0 && !client_msgs[3].equals("null")) {
                // PUT

				this.server.addToCache(client_msgs[0],client_msgs[3]);
				cacheSuccess = 1;


                FileStoreHelper.FileStoreStatusType result = fileStoreHelper.PutInFile(client_msgs[0], client_msgs[3]);
                if(result == FileStoreHelper.FileStoreStatusType.PUT_SUCCESS)
                {
                    fileSuccess = 1;
                }
            } else {
			    // UPDATE OR DELETE
                if(client_msgs[3].equals("null")){
                    // DELETE

                    FileStoreHelper.FileStoreStatusType result = fileStoreHelper.DeleteInFile(client_msgs[0]);

                    if(result == FileStoreHelper.FileStoreStatusType.DELETE_SUCCESS){

                        fileSuccess = 1;
                        this.server.getKvcache().deleteFromCache(client_msgs[0]);

                    }
                } else {
                    // UPDATE

                    FileStoreHelper.FileStoreStatusType result = fileStoreHelper.UpsertInFile(client_msgs[0], client_msgs[3]);

                    if(result == FileStoreHelper.FileStoreStatusType.UPSERT_SUCCESS){
                        fileSuccess = 1;

						this.server.getKvcache().insertInCache(client_msgs[0], client_msgs[3]);
                    }
                }
            }


			///
			if (fileSuccess == 1){
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
	
    public void sendMessage(byte[] msg, int len) throws IOException {
		output.write(msg, 0, len);
		output.flush();
		String message = new String(msg,0,len);
		if(log) {
			logger.info("Send message:\t '" + message + "'");
		}
    }
    private TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];//need to add in BUFFER_SIZE constant
		
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
		if(log) {
			logger.info("Receive message:\t '" + msg.getMsg() + "'");
		}
		return msg;
    }
}


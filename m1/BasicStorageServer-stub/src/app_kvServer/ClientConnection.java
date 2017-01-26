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
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
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
					System.out.println("command: "+latestMsg.getMsg().trim()+"type:"+(latestMsg.getMsg().trim()).getClass().getName());
					if (latestMsg.getMsg().trim().contains("P")){
						System.out.println("handling put");
						handle_put();
						
					}
					else if (latestMsg.getMsg().trim().contains("G")){
						
						System.out.println("handling get");
						handle_get();
					}
				//	sendMessage(latestMsg);
					

				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.info("Error! Connection lost!");
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
		System.out.println("key: "+key);
		logger.info("Client tried to get key: '"+key+"'");
		try{
			if (key.length()>20){
				throw new Exception("Client sent too large a key, key = '"+key+"', size = "+key.length());
			}
			//check if we have the key in our file
//			String payload = this.server.findInCache(key);
			System.out.println("NEED TO IMPLEMENT CHECK FOR KEY and retrieval of size and value");
			int got_key = 1;//change to be based on whether got the value
			String payload = "IMPLEMENT ME";
			int length = 12;//get me from file
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
					System.out.println("adding "+length_str.charAt(i));
					message[3+kl+i]=(byte) length_str.charAt(i);
				}
				message[3+kl+ll] = (byte) 0;
				for (int i = 0; i < length; i ++){
					message[4+kl+ll+i] = (byte) payload.charAt(i);
				}
				message[4+kl+ll+length]=(byte)0;
				System.out.println("sending");
				this.sendMessage(message, 5+kl+ll+length);
				System.out.println("sent");
			}
			else{
				byte[] message = new byte[2];
				message[0]=(byte) 'F';
				message[1]=(byte) 0;
				this.sendMessage(message, 2);
				String msg = "Get, client sent non-existent key, key = '"+key+"'";
	        	logger.info(msg);
	        	throw new Exception(msg);
			}
		}
		catch(Exception ex){
			logger.info(ex.getMessage());
		}
	
		
		
	}
	public void handle_put(){
		String [] client_msgs = new String[4];
		
		try{
			for (int i =0; i<2; i++){
				client_msgs[i] = this.receiveMessage().getMsg().trim();
				System.out.println("recieved "+client_msgs[i]);
			}
			System.out.println("key: "+client_msgs[0]);
			System.out.println("size: "+client_msgs[1]);
			logger.info("Put, client wants to place "+client_msgs[1]+" bytes for key '"+client_msgs[0]+"'");
			int kl = client_msgs[0].length();
			int ll = client_msgs[1].length();
			System.out.println("validating lengths");
			if (kl>20){
				System.out.println("long key");
				byte [] message = new byte [2];
				message[0]=(byte) 'F';
				message[1] = (byte) 0;
				this.sendMessage(message, 2);
				throw new Exception("Put, client sent too long of a key, key = '"+client_msgs[0]+"', length = "+kl);
			}
			System.out.println("key not too long");
			System.out.println(Integer.valueOf(client_msgs[1].trim()));
			if (Integer.valueOf(client_msgs[1].trim())>(120*1024)){
				System.out.println("long message");
				byte [] message = new byte [2];
				message[0]=(byte) 'F';
				message[1] = (byte) 0;
				this.sendMessage(message, 2);
				throw new Exception("Put, client sent too large a size, size = "+client_msgs[1]);
			}
			//GET the key from the file on disk populate below variables based on file
			System.out.println("NEED TO IMPLEMENT CHECK FOR KEY and retrieval of size and value");
			int got_key = 1;//change to be based on whether got the value
			int offset = 323; //use for quick location since will need to update value
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
				System.out.println("recieved" + client_msgs[i]);
			}
			System.out.println("flag: "+client_msgs[2]);
			System.out.println("payload: "+client_msgs[3]);
			if (client_msgs[2].contains("F")){
				throw new Exception("Put, client sent a failure signal");
			}
			if(client_msgs[3].length() != Integer.parseInt(client_msgs[1].trim())){
				throw new Exception("Put, client sent a payload of the incorrect size, expected "+client_msgs[1]+", got "+client_msgs[3].length());
			}
			///overwrite the payload or whatever other file stuff her
			System.out.println("NEED TO IMPLEMENT INSERTION, UPDATE and Deletion IN FILE");
			this.server.addToCache(client_msgs[0],client_msgs[3]);
			System.out.println("cached");
			int success = 1; //change based on insertion results
			///
			if (success == 1){
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
		catch(Exception ex){
			logger.info(ex.getMessage());
		}
		
		
	}
	
    public void sendMessage(byte[] msg, int len) throws IOException {
		output.write(msg, 0, len);
		output.flush();
		String message = new String(msg,0,len);
		logger.info("Send message:\t '" + message + "'");
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
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }
   
	/*private void sendMessage(String msg) throws IOException {
		int sizeOfMsg = msg.length();
		byte[] outputByteMessage = new byte[sizeOfMsg];
		for(int i = 0; i < sizeOfMsg; i++){
			outputByteMessage[i] = (byte)msg.charAt(i);
		}

		output.write(outputByteMessage);
		output.flush();
	}

	private String receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		byte firstByte = 0;

		int size = input.available();

		// if there is nothing to read block the thread until something becomes available
		if(size <= 0){
			firstByte = (byte)input.read(); // blocking call
			size = input.available();
			msgBytes = new byte[size+1];
			msgBytes[index] = firstByte;
			index++;
		}

		while(size > 0) {
			msgBytes[index] = (byte) input.read();
			index++;
			size = input.available();
		}


		// from this point onwards we have the entire byte array for the message
		// TODO: need to reconstruct fragmented packets since buffer is fixed size
		// TODO: need to discuss how we will fragment packets

		String command = getCommandFromByte(msgBytes[0]);
		String msg = null;

		if(command == "GET"){

			handleGet(getKeyFromBytes(msgBytes, 2));
		} else if (command == "PUT"){
			String k = getKeyFromBytes(msgBytes, 2);
			String v = getValueFromBytes(msgBytes, 22);
			handlePut(k, v);
		} else {
			msg = "Invalid Command";
		}
		index = 0; //reset the index for the next message from client

		return msg;
    }*/

	String getCommandFromByte(byte firstByte){
		String command = null;

		if((char)firstByte == 'G' ){
			return "GET";
		} else if ((char) firstByte == 'P'){
			return "PUT";
		}

		return command;
	}

	/*private void handleGet(String k)
	{
		try {
			String val = this.server.findInCache(k);
			String msg;

			//search in cache
			if (val != null) {
				msg = "Get was successful: " + "(" + k + ", " + val + ")";
			} else {
				//add logic to search from file
				msg = "Key: " + k + " not found";
			}

			try {
				this.sendMessage(msg);
			} catch (Exception ex) {
				logger.trace(ex.getMessage());
			}
		}catch(Exception ex){
			logger.trace(ex.getMessage());
		}
	}*/

	private String getKeyFromBytes(byte[] bts, int idx)
	{
		char[] byteKey = new char[20];
		for(int i =0; i < 20; i++)
		{
			byteKey[i] = (char)bts[i];
		}
		return new String(byteKey);
	}

	private String getValueFromBytes(byte[] bts, int idx)
	{
		char[] byteValue = new char[120000];
		for(int i =0; i < bts.length - 22; i++)
		{
			byteValue[i] = (char)bts[i];
		}
		return new String(byteValue);
	}

	/*private void handlePut(String k, String v)
	{
		this.server.addToCache(k,v);

		String msg = "Put success, Key:" + k + ", Value: " + v;

		try {
			this.sendMessage(msg);
		} catch (Exception ex) {
			logger.trace(ex.getMessage());
		}

		//add logic that adds k,v to the file
	}*/
}


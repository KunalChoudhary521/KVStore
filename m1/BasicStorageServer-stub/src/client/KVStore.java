package client;


import app_kvClient.ClientSocketListener;
import app_kvClient.ClientSocketListener.SocketStatus;
import app_kvClient.TextMessage;
import common.messages.KVMessage;
import javafx.animation.Animation;
import org.apache.log4j.Logger;
import client.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.Set;

import java.io.*;//remove me later

public class KVStore implements KVCommInterface {

	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private String address;
	private int port;

	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	//@Override
	public void connect() throws Exception {
		this.clientSocket = new Socket(address, port);

		try {
			this.output = clientSocket.getOutputStream();
			this.input = clientSocket.getInputStream();
		} catch(Exception ex){
			logger.info(ex.getMessage());//this.logger.trace(ex.getMessage());
		}

		if(clientSocket.isConnected()){
			String message = "Connection established successffully at " + new Date().toString();
			logger.info(message);//this.logger.trace(message);
		} else {
			String message = "Could not connect to host " + this.address + ":"
					+ this.port + "at " + new Date().toString();
			logger.info(message);//this.logger.trace(message);
			throw new Exception(message);
		}
	}

	//@Override
	public void disconnect() {
		String message = "try to close connection with " + this.address +":" + this.port
	    		+ "at" + new Date().toString();
	    	logger.info(message);
	    	
	    	try{
	    		tearDownConnection();
	    		for (ClientSocketListener listener : listeners){
	    			listener.handleStatus(SocketStatus.DISCONNECTED);
	    		}
	    	}catch(Exception ioe){
	    		logger.info("Unable to close connection!");
	    	}
	}
	private void tearDownConnection() throws IOException{
    	setRunning(false);
    	String message = "tearing down connection "+this.address +":"+this.port+"at"+new Date().toString();
    	logger.info(message);
    	if (clientSocket != null){
    		input.close();
    		output.close();
    		clientSocket.close();
    		clientSocket =null;
    		message = "connection:"+this.address +":"+this.port
    				+ "closed at" + new Date().toString();
    	}
    }
    public boolean isRunning() {
		return running;
	}
    public void setRunning(boolean run) {
		running = run;
	}
    public void addListener(ClientSocketListener listener){
		listeners.add(listener);
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
		logger.info("Receive message:\t '" + msg.getMsg().trim() + "'");
		return msg;
    }

	//@Override
	public KVMessage put(String key, String value) throws Exception {
		try{
			if(this.clientSocket == null){
				String msg = "Put, no client socket: key = '"+key+"', value = ' "+value+"'";
	        	logger.info(msg);
	        	throw new Exception(msg);
			}
			if(!this.clientSocket.isConnected()){
				String msg = "Put, client socket not connected: key = '"+key+"', value = ' "+value+"'";
	        	logger.info(msg);
	        	throw new Exception(msg);
			}
			int kl = key.length();
			int vl;
			if (value != null)
				vl = value.length();
			else
				vl = 0;
	        if (kl>20){
	        	String msg = "key: '" + key + "' too long, ("+kl+" bytes)";
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
	        if (vl > 120*1024){
	        	String msg = "value: '"+value+"' too long, ("+vl+" bytes)";
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
	        String length = Integer.toString(vl);
	        int ll = length.length();
	        int ml = kl+4+ll;
	        byte[] message = new byte[ml];
	        //byte[] byteKey = key.getBytes();//removed per piazza
	        
			byte[] byteKey = new byte[kl];
			for(int i =0; i < kl; i++){
				byteKey[i]=(byte)key.charAt(i);
			}
	        //byte[] length_byte = length.getBytes();//removed per piazza
			byte[] length_byte = new byte[ll];
			for(int i =0; i < ll; i++){
				length_byte[i]=(byte)length.charAt(i);
			}
	        TextMessage[] ret_vals = new TextMessage[4];
	        message[0] = (byte) 'P';
	        message[1] = (byte) 0;
	        for (int i = 0; i<kl; i++){
	        	message[2+i]=byteKey[i];
	        }
	        message[2+kl]=0;
	        for (int i = 0; i<ll;i++){
	        	System.out.println("adding"+length_byte[i]);
	        	message[3+kl+i]=length_byte[i];
	        }
	        message[3+kl+ll]= (byte)0;
	        this.sendMessage(message,4+kl+ll);
	        System.out.println("sent");
	        for (int i =0; i<3; i++){
	        	ret_vals[i]=this.receiveMessage();
	        	if (ret_vals[0].getMsg().trim().contains("F"))
	        		break;
	        }
	        System.out.println("flag: "+ret_vals[0].getMsg().trim());
	        System.out.println("key: "+ret_vals[1].getMsg().trim());
	        System.out.println("size: "+ret_vals[2].getMsg().trim());
	        if (ret_vals[0].getMsg().trim().contains("F")){
	        	String msg = "Put, server sent F when validating key: '"+key+"', disconnecting";
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
			System.out.println("server did not send F");
	        if (!ret_vals[1].getMsg().trim().equals(key) || 
	        		!ret_vals[2].getMsg().trim().equals(length)){
	        	byte[] failure = new byte[2];
	        	failure[0]=(byte) 'F';
	        	failure[1]=0;
	        	this.sendMessage(failure, 2);
	        	String msg = "Put, server responded with incorrect key or size: "
	        			+ ret_vals[1].getMsg().trim() +", " + ret_vals[2].getMsg().trim()+", disconnecting";
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
			System.out.println("server sent correct key and size");
	        message = null;
	        byte[] message2 = new byte[vl+3];
	        //byte [] payload_bytes = value.getBytes();//removed per piazza
			byte[] payload_bytes = new byte[vl];
			for(int i =0; i < vl; i++){
				payload_bytes[i]=(byte) value.charAt(i);
			}
	        message2[0]=(byte) 'S';
	        message2[1]=0;
	        for (int i =0; i<vl; i++){
	        	message2[2+i]=payload_bytes[i];
	        }
	        message2[2+vl]=0;
			System.out.println("sending payload");
	        this.sendMessage(message2, 3+vl);
			System.out.println("awaiting ack");
	        ret_vals[3]=this.receiveMessage();
	        System.out.println("ack: "+ret_vals[3].getMsg().trim());
	        if (ret_vals[3].getMsg().trim().contains("F")){
	        	String msg = "Put, server sent F after inserting: "
	        			+key +" : "+value+", disconnecting";
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
	        if (ret_vals[0].getMsg().trim().contains("S")){
	        	return new Message(key, value, KVMessage.StatusType.PUT_SUCCESS);
	        }
	        else{
	        	if (value.equals("null")){
	        		System.out.println("delete_success");
	        		return new Message(key, "null", KVMessage.StatusType.DELETE_SUCCESS);
	        	}
	        	else{
	        		System.out.println("updated");
	        		return new Message(key, value, KVMessage.StatusType.PUT_UPDATE);
	        	}
	        }
		}
		catch(Exception ex){
        		logger.info(ex.getMessage());
			if (ex.getMessage().contains(", disconnecting")){
        			this.disconnect();
	        	}
		
        		if (value.equals("null")){
	        		return new Message(key, value, KVMessage.StatusType.DELETE_ERROR);
	        	}
        		else{
	        		return new Message(key, value, KVMessage.StatusType.PUT_ERROR);
	        	}
        	}
	}

	//@Override
	public KVMessage get(String key) throws Exception {
		try{
			if(this.clientSocket != null){
				if(this.clientSocket.isConnected())
				{
					byte[] message = new byte[27];
					int kl = key.length();
					byte[] byteKey = new byte[kl];
					for(int i =0; i < kl; i++){
						byteKey[i]=(byte)key.charAt(i);
					}
//					byte[] byteKey = key.getBytes();//removed per piaza
					TextMessage[] ret_vals = new TextMessage[4];

	
					// fill the message with the proper command byte
					message[0] = (byte) 'G';
	
					// pad or align
					message[1] = (byte) 0;
					// fill the get message with a 20 byte key
					for(int i = 0; i < key.length(); i++)
					{
						message[2+i] = byteKey[i];
					}
					message[2+key.length()]=0;
					//message[3+key.length()]=0;
					this.sendMessage(message,3+key.length());
					for (int i =0; i<4;i++){
						ret_vals[i]=this.receiveMessage();
						if (ret_vals[i].getMsg().trim().contains("F"))
							break;
					}
					System.out.println("flag: "+ret_vals[0].getMsg().trim());
					System.out.println("key: "+ret_vals[1].getMsg().trim());
					System.out.println("size: "+ret_vals[2].getMsg().trim());
					System.out.println("payload: "+ret_vals[3].getMsg().trim());
					if (ret_vals[0].getMsg().trim().contains("F")){
						String msg = "Get, server sent F when validating key: '"+key+"'";
			        	logger.info(msg);
			        	return new Message(key, msg, KVMessage.StatusType.GET_ERROR);
					}
					else if(!ret_vals[1].getMsg().trim().equals(key)){
						String msg = "Get, server sent incorrect key: key="
			        			+ key + "returned key = " + ret_vals[1].getMsg().trim();
						logger.info(msg);
						return new Message(key, msg, KVMessage.StatusType.GET_ERROR);
					}
					else if(Integer.parseInt(ret_vals[2].getMsg().trim())!=ret_vals[3].getMsg().trim().length()){
						String msg = "Get, server sent either incorrect payload of incorrect size: payload="
			        			+ret_vals[3].getMsg().trim()+", size="+ret_vals[2].getMsg().trim();
						logger.info(msg);
						return new Message(key, msg, KVMessage.StatusType.GET_ERROR);
					}
					logger.info("payload = "+ret_vals[3].getMsg().trim());

				return new Message(key, ret_vals[3].getMsg().trim(), KVMessage.StatusType.GET_SUCCESS);
				}
			}
		}
		catch(Exception ex){
        	this.disconnect();
        	logger.info(ex.getMessage());
        	return new Message(key, null, KVMessage.StatusType.GET_ERROR);
        }
	return null;}
}

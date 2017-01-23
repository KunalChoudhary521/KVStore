package client;


import app_kvClient.ClientSocketListener;
import app_kvClient.ClientSocketListener.SocketStatus;
import app_kvClient.TextMessage;
import common.messages.KVMessage;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.Set;

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
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }

	//@Override
	public KVMessage put(String key, String value) throws Exception {
/*		KVMessage ret_val = new KVMessage();//change
    	ret_val.key = key;
    	rev_val.value=value;
        try{
	    	int kl = key.length();
	        int vl = value.length();
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
	    	this.sendMessage(new TextMessage("P"));
	        this.sendMessage(new TextMessage(key));
	        this.sendMessage(new TextMessage("\0"));
	        this.sendMessage(new TextMessage(Integer.toString(vl)));
	        TextMessage stat = this.receiveMessage();
	        if (stat.getMsg().equals("F")){
	        	String msg = "Put, server sent F when validating key: '"+key+"'";
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
	        TextMessage ret_key = this.receiveMessage();
	        assert this.receiveMessage().getMsg().equals("\0");
	        TextMessage ret_size = this.receiveMessage();
	        if (!key.equals(ret_key.getMsg()) || vl != Integer.parseInt(ret_size.getMsg())){
	        	this.sendMessage(new TextMessgae("F"));
	        	String msg = "Put, server responded with incorrect key or size: "
	        			+ ret_key.getMsg() +", " + ret_size.getMsg();
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
	        this.sendMessage(new TextMessage("S"));
	        this.sendMessage(new TextMessage(value));
	        TextMessage ret_final_stat = this.receiveMessage();
	        if (receiveMessage().getMsg().equals("F")){
	        	String msg = "Put, server sent F after inserting: "
	        			+key +" : "+value;
	        	logger.info(msg);
	        	throw new Exception(msg);
	        }
	        if (stat.getMsg().equals("S")){
	        	ret_val.stat = PUT_SUCCESS;
	        }
	        else{
	        	if (value.equals(null)){
	        		ret_val.stat = DELETE_SUCCESS;
	        	}
	        	else{
	        		ret_val.stat = PUT_UPDATE;
	        	}
	        	
	        }
	    	return ret_val;
        }
        catch(Exception ex){
        	if (ex.getMessage().contains("Put, server")){
        		this.disconnect();
        	}
        	if (value.contentEquals(null)){
        		ret_val.stat = DELETE_ERROR;
        	}
        	else{
        		ret_val.stat = PUT_ERROR;
        	}
        	return ret_val;
        }*/
		return null;
	}

	//@Override
	public KVMessage get(String key) throws Exception {
		try{
			if(this.clientSocket != null){
				if(this.clientSocket.isConnected())
				{
					byte[] message = new byte[27];
	
					byte[] byteKey = key.getBytes();
					TextMessage[] ret_vals = new TextMessage[4];
					/*for(int i = 0; i < 20; i++){
						byteKey[i] = (byte) 'K';
					}*/
	
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
					message[3+key.length()]=0;
					this.sendMessage(message,4+key.length());
					for (int i =0; i<4;i++){
						ret_vals[i]=this.receiveMessage();
					}
					if (ret_vals[0].getMsg().equals("F")){
						String msg = "Get, server sent F when validating key: '"+key+"'";
			        	logger.info(msg);
			        	throw new Exception(msg);
					}
					else if(!ret_vals[1].getMsg().equals(key)){
						String msg = "Get, server sent incorrect key: key="
			        			+ key + "returned key = " + ret_vals[1].getMsg();
			        	logger.info(msg);
			        	throw new Exception(msg);
					}
					else if(Integer.parseInt(ret_vals[2].getMsg())!=ret_vals[3].getMsg().length()){
						String msg = "Get, server sent either incorrect payload of incorrect size: payload="
			        			+ret_vals[3].getMsg()+", size="+ret_vals[2].getMsg();
			        	logger.info(msg);
			        	throw new Exception(msg);
					}
					logger.info("payload = "+ret_vals[3].getMsg());
	/*				// pad or align again
					message[22] = (byte) 0;
	
					int size = 32;
					char[] charSize =	Integer.toString(size).toCharArray();
	
					for(int i = 0; i < charSize.length; i++){
						message[22+i] = (byte) charSize[i];
					}
					this.clientSocket.getOutputStream().write(message, 0, message.length);
					//this.clientSocket.getOutputStream().flush();
					 * 
					 */
				}
			}
		}
		catch(Exception ex){
        	this.disconnect();
        	logger.info(ex.getMessage());
        	//set state to GET_ERROR;
        }
		return null;
	}

	
}

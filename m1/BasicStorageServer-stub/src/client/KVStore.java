package client;


import app_kvClient.ClientSocketListener;
import common.messages.KVMessage;
import javafx.animation.Animation;
import org.apache.log4j.Logger;

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

	@Override
	public void connect() throws Exception {

		this.clientSocket = new Socket(address, port);
		this.input = clientSocket.getInputStream();
		this.output = clientSocket.getOutputStream();

		if(clientSocket.isConnected()){
			String message = "Connection established successffully at " + new Date().toString();
			this.logger.trace(message);
		} else {
			String message = "Could not connect to host " + this.address + ":"
					+ this.port + "at " + new Date().toString();
			this.logger.trace(message);
			throw new Exception(message);
		}
	}

	@Override
	public void disconnect() {

	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if(this.clientSocket != null){
			if(this.clientSocket.isConnected())
			{
				byte[] message = new byte[27];

				byte[] byteKey = new byte[key.length()];
				for(int i = 0; i < key.length(); i++){
					byteKey[i] = (byte) key.charAt(i);
				}

				// fill the message with the proper command byte
				message[0] = (byte) 'G';

				// pad or align
				message[1] = (byte) 0;

				// fill the get message with a 20 byte key
				for(int i = 0; i < byteKey.length; i++)
				{
					message[2+i] = byteKey[i];
				}

				// pad or align again
				message[22] = (byte) 0;

				int size = 32;
				char[] charSize =	Integer.toString(size).toCharArray();

				for(int i = 0; i < charSize.length; i++){
					message[22+i] = (byte) charSize[i];
				}
				this.output.write(message, 0, message.length);
				this.output.flush();

				// client blocks until a response from the server is received
				String serverReply = getResponse();

				Message responseMessage;

				//TODO construct KVMessage correctly
				if(serverReply != null){
					responseMessage = new Message(serverReply, serverReply, KVMessage.StatusType.GET_SUCCESS);
				} else {
					responseMessage = new Message(null, null, KVMessage.StatusType.GET_ERROR);
				}

				return responseMessage;
			}
		}
		return null;
	}

	private String getResponse(){
		try {
			byte firstByte = 0;
			int responseSize = this.input.available();
			byte[] msgBytes = null;
			int index = 0;

			// if there is nothing to read block the thread until something becomes available
			if (responseSize <= 0) {
				firstByte = (byte) input.read(); // blocking call
				responseSize = input.available();
				msgBytes = new byte[responseSize + 1];
				msgBytes[index] = firstByte;
				index++;
			} else {
				msgBytes = new byte[responseSize];
			}

			while (responseSize > 0) {
				msgBytes[index] = (byte) input.read();
				index++;
				responseSize = input.available();
			}

			char[] charMsg = new char[msgBytes.length];
			for(int i = 0; i < msgBytes.length; i++){
				charMsg[i] = (char) msgBytes[i];
			}

			return new String(charMsg);

		}
		catch(Exception ex){
			return null;
		}
	}
}

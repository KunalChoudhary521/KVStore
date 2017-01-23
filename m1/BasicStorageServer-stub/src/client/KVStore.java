package client;


import app_kvClient.ClientSocketListener;
import common.messages.KVMessage;
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

		try {
			this.output = clientSocket.getOutputStream();
			this.input = clientSocket.getInputStream();
		} catch(Exception ex){
			this.logger.trace(ex.getMessage());
		}

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

				byte[] byteKey = new byte[20];
				for(int i = 0; i < 20; i++){
					byteKey[i] = (byte) 'K';
				}

				// fill the message with the proper command byte
				message[0] = (byte) 'G';

				// pad or align
				message[1] = (byte) 0;

				// fill the get message with a 20 byte key
				for(int i = 0; i < 20; i++)
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
				this.clientSocket.getOutputStream().write(message, 0, message.length);
				//this.clientSocket.getOutputStream().flush();
			}
		}
		return null;
	}

	
}

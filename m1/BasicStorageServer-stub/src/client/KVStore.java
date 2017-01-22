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
		return null;
	}

	
}

package app_kvServer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


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
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
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
					String latestMsg = receiveMessage();
					sendMessage(latestMsg);

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

	private void sendMessage(String msg) throws IOException {
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
			msg = "Get was successful: message from client was, " + new String(msgBytes);
		} else if (command == "PUT"){
			msg = "Put was successful";
		} else {
			msg = "Invalid Command";
		}
		index = 0; //reset the index for the next message from client

		return msg;
    }

	String getCommandFromByte(byte firstByte){
		String command = null;

		if((char)firstByte == 'G' ){
			return "GET";
		} else if ((char) firstByte == 'P'){
			return "PUT";
		}

		return command;
	}
	
}

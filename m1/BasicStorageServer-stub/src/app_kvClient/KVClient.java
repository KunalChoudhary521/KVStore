package app_kvClient;


import client.KVCommInterface;
import client.KVStore;
import common.messages.KVMessage;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Time;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static javax.swing.text.html.HTML.Tag.HEAD;

public class KVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore client = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    client = new KVStore(serverAddress, serverPort);
                    client.connect();
                    println("Connected to server successfully");
                } catch (Exception ex) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", ex);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("disconnect")) {
            disconnect();
        } else if(tokens[0].equals("getTest")) {
            try {
                KVMessage response = client.getTest(tokens[1]);
                if (response.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                    println("Get Success-----");
                    println("Key:\t" + response.getKey());
                    println("Value:\t" + response.getValue());
                    println("----------------");
                }

            }
            catch(Exception ex) {
                printError("Get failed.");
            }
        } else if(tokens[0].equals("put")){
            try{
		    /*next lines to replace putTest*/
		    println("tokens: "+ tokens.length);
		    KVMessage response = client.put(tokens[1], tokens[2]);
		     if(response.getStatus() == KVMessage.StatusType.PUT_SUCCESS){
 			println("Put Success-----");
                   	println("Key:\t" + response.getKey());
	                println("Value:\t" + response.getValue());
         	        println("----------------");
		     }else if(response.getStatus() == KVMessage.StatusType.PUT_UPDATE){
			println("Update completed successfully");
                   	println("Key:\t" + response.getKey());
	                println("Value:\t" + response.getValue());
         	        println("----------------");
		     }else if(response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS){
			println("Delete completed successfully");
                   	println("Key:\t" + response.getKey());
         	        println("----------------");
		     }else if(response.getStatus() == KVMessage.StatusType.PUT_ERROR){
			println("Put failed please try again");
			if(tokens[1].length()>20)
				println("Your key was too long, maximum length is 20 characters");
			else if (tokens[2].length()>120*1024){
				println("your payload was too long, maximum length is 122880 characters");
			}
                   	println("Attempted Key:\t" + response.getKey());
	                println("Attempted Value:\t" + response.getValue());
         	        println("----------------");
		     }else if (response.getStatus() == KVMessage.StatusType.DELETE_ERROR){
			println("Delete failed please try again");
			if(tokens[1].length()>20)
				println("Your key was too long, maximum length is 20 characters");
                   	println("Attempted Key:\t" + response.getKey());
         	        println("----------------");

		     }


		   /*  
                KVMessage response = client.putTest(tokens[1], tokens[2]);
                if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                    println("Put Success-----");
                    println("Key:\t" + response.getKey());
                    println("Value:\t" + response.getValue());
                    println("----------------");
                }*/

            } catch (Exception ex) {
                printError("Put failed.");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
	}else if (tokens[0].equals("get")){
		try{
			KVMessage response = client.get(tokens[1]);
			if (response.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
				println("Get Success-----");
				println("Key:\t" + response.getKey());
				println("Value:\t" + response.getValue());
				println("----------------");
	                }else if(response.getStatus() == KVMessage.StatusType.GET_ERROR){
				println("Get Failure------");
				if (response.getKey().length()<=20)
					println("Key:\t" +response.getKey() + "not found, please"+
					    "check the key and try again");
				else
					println("Key:\t" +response.getKey() + "too long, please"+
					    "check the key and try again. Maximum key"+
					    " length is 20 characters.");
			}
		}
		catch(Exception ex){
			println("get failed. "+ex.getMessage());
		}
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KVClient HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t disconnects from the server \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t exits the program\n");
	sb.append(PROMPT).append("put <key> <value>");
	sb.append("\t places a key value pair on the server.");
	sb.append(" If the key is already on the server, replaces");
	sb.append(" the old value with the new value. If the value is null");
	sb.append(", removes the key-value pair from the server. Maximum key length");
	sb.append(" is 20 characters. Maximum value lenth is 122880 characters\n");
	sb.append(PROMPT).append("get <key>");
	sb.append("\t retrieves a key value pair from the server. Maximum key");
	sb.append("length is 20 characters.\n");


        System.out.println(sb.toString());
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    private void disconnect() {
        if(client != null) {
            try {
                client.disconnect();
            } catch(Exception ex){
                println(ex.getMessage());
            }
            client = null;
        }
    }

    public static void main(String args[]){
        try {
            KVClient app = new KVClient();
            app.run();
        } catch (Exception ex){
            System.out.println("Error! Unable to initialize logger!");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static void println(String message){
        System.out.println(message);
    }
}

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
        } else if(tokens[0].equals("getTest")){
            try {
                client.get("blah");
            } catch(Exception ex) {
                printError("Get failed.");
            }
        } else if(tokens[0].equals("help")) {
            printHelp();
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
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
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

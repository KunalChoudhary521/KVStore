package app_kvClient;


import client.KVStore;
import common.messages.KVMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.time.LocalTime;

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
            logger.trace("client UI running");
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                logger.debug("About to get user input");
                String cmdLine = stdin.readLine();
                logger.debug("User input "+cmdLine);
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
                logger.fatal("CLI does not respond - Application terminated "+e);
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            //logger.info("disconnecting at " +Time.valueOf(LocalTime.now()));
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try {
                    //logger.debug("Connecting to "+tokens[1]+":"+tokens[2]
                    //+"at"+Time.valueOf(LocalTime.now()));
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    client = new KVStore(serverAddress, serverPort);
                    client.connect();
                   // logger.info("Connecting to "+tokens[1]+":"+tokens[2]
                   //         +"at"+Time.valueOf(LocalTime.now()));
                    println("Connected to server successfully");
                } catch (Exception ex) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", ex);
                }
            } else {
                //logger.error("Invalid number of parameters for connection at "
                //        +Time.valueOf(LocalTime.now()));
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("disconnect")) {
            //logger.info("Disconnecting from "+serverAddress+":"+serverPort
             //       +"at"+Time.valueOf(LocalTime.now()));
            disconnect();
            //logger.debug("Disconnected from "+serverAddress+":"+serverPort
             //       +"at"+Time.valueOf(LocalTime.now()));
        }else if(tokens[0].equals("logLevel")){
            //logger.info("Changing logging level to "+tokens[1]
             //       +"at"+Time.valueOf(LocalTime.now()));
            //logger.setLevel(Level.toLevel(tokens[1]));
            System.out.println("logger level now" + logger.getLevel());
        }else if(tokens[0].equals("put")){
            try{
                //logger.debug("Put, # tokens: "+ tokens.length);
                //logger.debug("Put, tokens = : "+ tokens[1]+"and"+ tokens[2]);

                KVMessage response = client.put(tokens[1], tokens[2]);
                //logger.debug("Put, returned at: "+ Time.valueOf(LocalTime.now()));

                if(response.getStatus() == KVMessage.StatusType.PUT_SUCCESS){
                    //logger.info("Put was successful, key was"+response.getKey()+
                    //    "value was"+response.getValue());
                    println("Put Success-----");
                    println("Key:\t" + response.getKey());
                    println("Value:\t" + response.getValue());
                    println("----------------");
                 }else if(response.getStatus() == KVMessage.StatusType.PUT_UPDATE){
                    //logger.info("Update was successful, key was"+response.getKey()+
                    //        "value was"+response.getValue());
                    println("Update completed successfully");
                    println("Key:\t" + response.getKey());
                    println("Value:\t" + response.getValue());
                    println("----------------");
                 }else if(response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS){
                    //logger.info("Delete was successful, key was"+response.getKey());
                    println("Delete completed successfully");
                    println("Key:\t" + response.getKey());
                    println("----------------");
                 }else if(response.getStatus() == KVMessage.StatusType.PUT_ERROR){
                     //logger.error("Put failed at "+ Time.valueOf(LocalTime.now()));
                     println("Put failed please try again");
                     if(tokens[1].length()>20) {
                         //logger.error("Reason for failure was key was too long");
                         println("Your key was too long, maximum length is 20 characters");
                     }else if (tokens[2].length()>120*1024){
                        //logger.error("Reason for failure was value was too long");
                         println("your payload was too long, maximum length is 122880 characters");
                     }
                     println("Attempted Key:\t" + response.getKey());
                     println("Attempted Value:\t" + response.getValue());
                     println("----------------");
                 }else if (response.getStatus() == KVMessage.StatusType.DELETE_ERROR){
                    //logger.error("Delete failed at "+ Time.valueOf(LocalTime.now()));
                    println("Delete failed please try again");
                    if(tokens[1].length()>20)
                        //logger.error("Reason for failure was key was too long");
                        println("Your key was too long, maximum length is 20 characters");
                        println("Attempted Key:\t" + response.getKey());
                        println("----------------");

                 }

            } catch (Exception ex) {
                printError("Put failed.");
                //logger.fatal("Put failed");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
	    }else if (tokens[0].equals("get")){
            //logger.debug("Get, # tokens: "+ tokens.length);
		    try{
		        //logger.info("concatenating elements of key");
                String fullKey = "";
                if(tokens.length > 2) {
                    for(int i = 1; i < tokens.length; i++){
                        fullKey = fullKey + tokens[i];
                        if(i != tokens.length - 1){
                            fullKey += " ";
                        }
                    }
                } else {
                    fullKey = tokens[1];
                }
                //logger.info("Getting"+fullKey);
                KVMessage response = client.get(fullKey);
                if (response.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                    //logger.info("Got"+fullKey+"at"+Time.valueOf(LocalTime.now())+
                    //"value was"+response.getValue());
                    println("Get Success-----");
                    println("Key:\t" + response.getKey());
                    println("Value:\t" + response.getValue());
                    println("----------------");
                }else if(response.getStatus() == KVMessage.StatusType.GET_ERROR){
                    //logger.info("Get failed, key ="+fullKey+"at"+
                                    //Time.valueOf(LocalTime.now()));
                    println("Get Failure------");
                    if (response.getKey().length()<=20)
                        println("Key:\t" +response.getKey() + " not found, please"+
                            "check the key and try again");
                    else
                        //logger.info("reason for failure was key was too long");
                        println("Key:\t" +response.getKey() + " too long, please"+
                            "check the key and try again. Maximum key"+
                            " length is 20 characters.");
                }
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("get failed. "+ex.getMessage());
            }
        } else {
	        //logger.error("unknown command at"+Time.valueOf(LocalTime.now()));
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
        sb.append(PROMPT).append("logLevel <level>");
        sb.append("sets the logLevel to the given level, options are: ");
        sb.append("ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF \n");


        System.out.println(sb.toString());
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    private void disconnect() {
        if(client != null) {
            try {
                //logger.info("Disconnecting at "+Time.valueOf(LocalTime.now()));
                client.disconnect();
            } catch(Exception ex){
                logger.error("disconnect failed",ex);
                println(ex.getMessage());
            }
            client = null;
        }
    }

    public static void main(String args[]){
        try {
            new LogSetup(System.getProperty("user.dir")+"/logs/client.log", Level.ALL);
            KVClient app = new KVClient();
            app.run();
        } catch (Exception ex){
            System.out.println("Error! Unable to initialize logger!");
            ex.printStackTrace();
            logger.fatal("setup issue",ex);
            System.exit(1);
        }
    }

    private static void println(String message){
        System.out.println(message);
    }
}

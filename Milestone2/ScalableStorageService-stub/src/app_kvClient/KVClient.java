package app_kvClient;

import client.KVStore;
import client.ClientSocketListener;
import common.TextMessage;

import common.messages.KVMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements ClientSocketListener
{
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore kvClient = null;
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

    private void handleCommand(String cmdLine)
    {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("connect"))
        {
            if(tokens.length == 3)
            {
                try
                {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    connect(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
            }
            else
            {
                printError("Invalid number of parameters!");
            }
        }
        else if(tokens[0].equals("disconnect"))
        {
            disconnect();
        }
        else if(tokens[0].equals("put"))
        {
            if(tokens.length == 3)
            {
                try
                {
                    KVMessage response = kvClient.put(tokens[1], tokens[2]);
                    if (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS)
                    {
                        logger.info("PUT SUCCESSFUL: <key,value>: " + "<" + response.getKey()
                                + "," + response.getValue() + ">");
                    }
                    else if (response.getStatus() == KVMessage.StatusType.PUT_ERROR)
                    {
                        logger.error("Error! PUT FAILED: <key>: " + "<" + response.getKey() + ">");
                    }
                    else if (response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS)
                    {
                        logger.info("DELETE SUCCESSFUL: <key>: " + "<" + response.getKey() + ">");
                    }
                    else if (response.getStatus() == KVMessage.StatusType.DELETE_ERROR)
                    {
                        logger.error("Error! DELETE FAILED: <key>: " + "<" + response.getKey() + ">");
                    }
                    else if (response.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK)
                    {
                        logger.error("Error! KVServer locked for put, only get allowed");
                    }
                }
                catch (Exception ex) {
                    logger.error("Error! PUT NOT successful");
                }
            }
            else
            {
                logger.error("Error! Invalid number of parameters!");
                logger.error("PUT Command format: put <key> <value>");
            }
        }
        else if(tokens[0].equals("get"))
        {
            if(tokens.length == 2)
            {
                try
                {
                    KVMessage response = kvClient.get(tokens[1]);
                    if (response.getStatus() == KVMessage.StatusType.GET_SUCCESS)
                    {
                        logger.info("GET SUCCESSFUL: <key,value>: " + "<" + response.getKey()
                                + "," + response.getValue() + ">");
                    }
                }
                catch (Exception ex) {
                    logger.error("Error! GET NOT successful");
                }
            }
            else
            {
                logger.error("Error! Invalid number of parameters!");
                logger.error("GET Command format: get <key>");
            }
        }
        else if(tokens[0].equals("logLevel"))
        {
            if(tokens.length == 2) {
                String level = setLogLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL))
                {
                    printError("No valid log level!");
                    System.out.println("Possible log levels are: " +
                            "(ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
                }
                else
                {
                    System.out.println(PROMPT + "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        }
        else if(tokens[0].equals("help"))
        {
            printHelp();
        }
        else if(tokens[0].equals("quit"))
        {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");
        }
        else
        {
            printError("Unknown command");
            printHelp();
        }
    }

    private void connect(String address, int port) throws IOException
    {
        kvClient = new KVStore(address, port);
        try {
            kvClient.connect();
        } catch (Exception ex) {
            logger.info("Connection with " + this.serverAddress +
                    ":" + this.serverPort + " not established");
        }
    }
    private void disconnect()
    {
        if(kvClient != null)
        {
            kvClient.disconnect();//disconnect of KVStore Class
            kvClient = null;
        }
    }

    private void printHelp()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <address> <port>");
        sb.append(addSpace(5));
        sb.append("establish a connection to a KVServer\n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append(addSpace(12));
        sb.append("sends a key-value pair to a KVServer \n");

        sb.append(PROMPT).append("get <key>");
        sb.append(addSpace(20));
        sb.append("sends a key-value pair to a KVServer \n");

        sb.append(PROMPT).append("disconnect");
        sb.append(addSpace(19));
        sb.append("disconnects from the server \n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append(addSpace(21));
        sb.append("changes the logLevel ");
        sb.append("(ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF) \n");

        sb.append(PROMPT).append("quit");
        sb.append(addSpace(25));
        sb.append("exits the program \n");

        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::");
        System.out.println(sb.toString());
    }

    private String setLogLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    @Override
    public void handleNewMessage(TextMessage msg) {
        if(!stop) {
            System.out.println(msg.getMsg());
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    private String addSpace(int numOfSpaces)
    {
        return new String(new char[numOfSpaces]).replace("\0", " ");
    }

    private void printError(String error)
    {
        System.out.println(PROMPT + "Error! " +  error);
    }

    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient app = new KVClient();
            app.run();

        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}

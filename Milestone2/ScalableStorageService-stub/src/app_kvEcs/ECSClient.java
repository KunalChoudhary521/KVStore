package app_kvEcs;

import ecs.ECS;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.messages.KVAdminMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

//analogous to KVClient.java
public class ECSClient
{
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private ECS ecsClient = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public ECSClient(String conf)
    {
        this.ecsClient = new ECS(conf);
    }

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
        KVAdminMessage response;

        if(tokens[0].equals("init") && tokens.length == 5)
        {
            response = this.ecsClient.initService(Integer.parseInt(tokens[1]), tokens[2],
                                        Integer.parseInt(tokens[3]),tokens[4]);
            if(response.getStatus() == KVAdminMessage.StatusType.INIT_SUCCESS)
            {
                //
            }
            else if(response.getStatus() == KVAdminMessage.StatusType.INIT_ERROR)
            {
                //
            }
        }
        else if(tokens[0].equals("start"))
        {
            response = this.ecsClient.start();
            if(response.getStatus() == KVAdminMessage.StatusType.START_SUCCESS)
            {
                //
            }
            else if(response.getStatus() == KVAdminMessage.StatusType.START_ERROR)
            {
                //
            }
        }
        else if(tokens[0].equals("stop"))
        {
            response = this.ecsClient.stop();
            if(response.getStatus() == KVAdminMessage.StatusType.STOP_SUCCESS)
            {
                //
            }
            else if(response.getStatus() == KVAdminMessage.StatusType.STOP_ERROR)
            {
                //
            }
        }
        else if(tokens[0].equals("shutdown"))
        {
            this.ecsClient.shutDown();
        }
        else if(tokens[0].equals("add") && tokens.length == 4)
        {
            response = this.ecsClient.addNode(tokens[1], Integer.parseInt(tokens[2]),
                                                tokens[3],true);
            if(response.getStatus() == KVAdminMessage.StatusType.ADD_SUCCESS)
            {
                logger.info("ADD NODE SUCCESSFUL: <" + response.getAddress() +
                            ":" + response.getPort() + ">");
            }
            else if(response.getStatus() == KVAdminMessage.StatusType.ADD_ERROR)
            {
                logger.info("ADD NODE FAILED: <" + response.getAddress() +
                        ":" + response.getPort() + ">");
            }
        }
        else if(tokens[0].equals("remove") && tokens.length == 3)
        {
            response = this.ecsClient.removeNode(tokens[1], Integer.parseInt(tokens[2]));
            if(response.getStatus() == KVAdminMessage.StatusType.REMOVE_SUCCESS)
            {
                logger.info("REMOVE NODE SUCCESSFUL: <" + response.getAddress() +
                        ":" + response.getPort() + ">");
            }
            else if(response.getStatus() == KVAdminMessage.StatusType.REMOVE_ERROR)
            {
                logger.info("REMOVE NODE FAILED: <" + response.getAddress() +
                        ":" + response.getPort() + ">");
            }
        }
        else if(tokens[0].equals("logLevel") && tokens.length == 2)
        {
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
        }
        else if(tokens[0].equals("help"))
        {
            printHelp();
        }
        else if(tokens[0].equals("quit"))
        {
            stop = true;
            System.out.println(PROMPT + "Application exit!");
        }
        else
        {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printHelp()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("init <numOfNodes> <logLevel> <cacheSize> <strategy>");
        sb.append(addSpace(5));
        sb.append("initialize and start <numOfNodes> KVServers from ecs.config\n");

        sb.append(PROMPT).append("start");
        sb.append(addSpace(28));
        sb.append("start initialized or stopped KVServers\n");

        sb.append(PROMPT).append("stop");
        sb.append(addSpace(29));
        sb.append("stop all KVServers (rejects get & put requests)\n");

        sb.append(PROMPT).append("shutdown");
        sb.append(addSpace(25));
        sb.append("stop all KVServer instances and exits the processes\n");

        sb.append(PROMPT).append("add <logLevel> <cacheSize> <strategy>");
        sb.append(addSpace(3));
        sb.append("add a KVServer from the available servers in ecs.config\n");

        sb.append(PROMPT).append("remove <address> <port>");
        sb.append(addSpace(6));
        sb.append("remove a KVServer at address:port\n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append(addSpace(25));
        sb.append("changes the logLevel ");
        sb.append("(ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF) \n");

        sb.append(PROMPT).append("quit");
        sb.append(addSpace(29));
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

    private String addSpace(int numOfSpaces)
    { return new String(new char[numOfSpaces]).replace("\0", " "); }

    private void printError(String error)
    { System.out.println(PROMPT + "Error! " +  error); }

    public static void main(String[] args)
    {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            //ECSClient ecsApp = new ECSClient(args[0]);  ecsApp.run();

            ECS test = new ECS("ecs.config");
            /*test.addNode("ALL",10,"LRU");
            test.addNode("ALL",10,"LRU");
            test.addNode("ALL",10,"LRU");
            test.addNode("ALL",10,"LRU");*/
            System.out.println("Done");

        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}

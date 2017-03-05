package app_kvEcs;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ECSClient {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private ECS client = new ECS(true);
    private boolean stop = false;

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

        for(int i =0;i<tokens.length;i++){
            System.out.println(tokens[i]);
        }

        if(tokens[0].equals("help")) {
            printHelp();
        }else if (tokens[0].equals("initService")){
            try{
                client.initService(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]),tokens[3]);
                println ("initialized");
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("Initialization failed. "+ex.getMessage());
            }
        }else if(tokens[0].equals("start")){
            try{
                client.start();
                println ("started");
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("start failed. "+ex.getMessage());
            }
        }else if(tokens[0].equals("stop")){
            try{
                client.stop();
                println ("stopped");
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("stop failed. "+ex.getMessage());
            }
        } else if(tokens[0].equals("shutDown")){
            try{
                client.shutDown();
                println ("shutDown successfull");
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("shutDown failed. "+ex.getMessage());
            }
        }else if(tokens[0].equals("addNode")){
            try{
                client.addNode(Integer.parseInt(tokens[1]), tokens[2]);
                println ("added");
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("add failed. "+ex.getMessage());
            }
        }else if(tokens[0].equals("removeNode")){
            try{
                client.removeNode(tokens[1],Integer.parseInt(tokens[2]));
                println ("removed");
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("remove failed. "+ex.getMessage());
            }
        }else if(tokens[0].equals("unlockWrite")){
            try{
                client.unlockWrite(tokens[1],Integer.parseInt(tokens[2]));
                println ("unlocked write on " + tokens[1]  + ", " + tokens[2]);
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("remove failed. "+ex.getMessage());
            }
        }else if(tokens[0].equals("lockWrite")){
            try{
                client.lockWrite(tokens[1],Integer.parseInt(tokens[2]));
                println ("locked write on " + tokens[1]  + ", " + tokens[2]);
            }
            catch(Exception ex){
                //logger.error("Get failed"+"at"+
                //        Time.valueOf(LocalTime.now()));
                println("remove failed. "+ex.getMessage());
            }
        }else{
            //logger.error("unknown command at"+Time.valueOf(LocalTime.now()));
            printError("Unknown command");
            printHelp();
        }
    }
    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECSClient HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("initService <number of nodes> <cache size> <replacement strategy>\n");
        sb.append("\t Randomly choose <numberOfNodes> servers from the available\n");
        sb.append("\t machines and start the KVServer by issuing an SSH call to the\n");
        sb.append("\t respective machine. This call launches the storage server with the\n");
        sb.append("\t specified cache size and replacement strategy. For simplicity\n");
        sb.append("\t locate the KVServer.jar in the same directory as the ECS. All\n");
        sb.append("\t storage servers are initialized with the metadata and remain in\n");
        sb.append("\t state stopped.\n");

        sb.append(PROMPT).append("start\n");
        sb.append("\t Starts the storage service by calling start() on all KVServer\n");
        sb.append("\t instances that participate in the service.\n");


        sb.append(PROMPT).append("stop\n");
        sb.append("\t Stops the service; all participating KVServers are stopped for\n");
        sb.append("\t processing client requests but the processes remain running.\n");


        sb.append(PROMPT).append("shutDown\n");
        sb.append("\t Stops all server instances and exits the remote processes.\n");


        sb.append(PROMPT).append("addNode <cache size> <replacement strategy>\n");
        sb.append("\t Create a new KVServer with the specified cache size and\n");
        sb.append("\t replacement strategy and add it to the storage service at an\n");
        sb.append("\t arbitrary position.\n");


        sb.append(PROMPT).append("removeNode <Server IP> <Server Port>");
        sb.append("\t Remove a server from the storage service.\n");

        sb.append(PROMPT).append("unlockWrite <Server IP> <Server Port>");
        sb.append("\t Enables puts on a server in the storage service.\n");

        sb.append(PROMPT).append("lockWrite <Server IP> <Server Port>");
        sb.append("\t Disables puts on a server in the storage service.\n");

        System.out.println(sb.toString());
    }
    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }
    private static void println(String message){
        System.out.println(message);
    }

    public static void main(String args[]){
        try {
            new LogSetup(System.getProperty("user.dir")+"/logs/ecs/ecsClient.log", Level.ALL);

            ECSClient app = new ECSClient();
            app.run();
        } catch (Exception ex){
            System.out.println("Error! Unable to initialize logger!");
            ex.printStackTrace();
            logger.fatal("setup issue",ex);
            System.exit(1);
        }
    }
}

package app_kvServer;

import cache.FIFOCache;
import cache.KVCache;
import cache.LFUCache;
import cache.LRUCache;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KVServer extends Thread
{

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private Path kvDirPath;
    private KVCache cache;
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	public KVServer(int port, String logLevel, int cacheSize, String strategy)
	{
		this.port = port;
        setLogLevel(logLevel);

        this.kvDirPath = Paths.get(String.valueOf(port));
        this.cache = setCacheType(cacheSize, strategy);
        if(this.cache == null)
        {
            logger.error("Error! No caching enabled");
        }

        try {
            createStorageObjects();
        }
        catch (Exception ex) {
            logger.error("Error! Unable to create directory /" + port);
            this.running = false;//terminate KVServer
        }
}

    private KVCache setCacheType(int cSize, String strat)
    {
        KVCache temp;
        if(cSize <= 0) {
            temp = null;
        } else if(strat.equals("FIFO")) {
            temp = new FIFOCache(cSize);
            logger.info("FIFO cache selected on KVServer @ port <" + this.port + ">");
        } else if(strat.equals("LFU")) {
            temp = new LFUCache(cSize);
            logger.info("LFU cache selected on KVServer @ port <" + this.port + ">");
        } else if(strat.equals("LRU")) {
            temp = new LRUCache(cSize);
            logger.info("LRU cache selected on KVServer @ port <" + this.port + ">");
        } else {
            temp = null;
        }
        return temp;
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

    public void createStorageObjects() throws IOException
    {
        if (Files.notExists(this.kvDirPath))
        {
            Files.createDirectory(this.kvDirPath);

            logger.info("KVServer on port <" + this.port + "> " + "New Directory: "
                    + System.getProperty("user.dir") + File.separator +
                    this.kvDirPath.toString());
        }
    }

    private boolean isRunning()
    {
        return this.running;
    }

    public void run()
    {
        this.running = initializeServer();
        if(serverSocket != null)
        {
            while(isRunning())
            {
                try
                {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client, this.kvDirPath, this.cache);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            +  " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        stopServer();
    }

    private boolean initializeServer()
    {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    public void stopServer()
    {
        logger.info("KVServer<" + serverSocket.getInetAddress().getHostAddress() + ":"
                + serverSocket.getLocalPort() + "> SHUTTING DOWN!!");
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    public static void main(String[] args) {
        try {
            //TODO: append start time of KVServer application to log file -> server-<timestamp>.log
            new LogSetup("logs/server.log", Level.ALL);//initially, logLevel is ALL
            if(args.length != 4) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port>!");
            } else {
                int port = Integer.parseInt(args[0]);
                String logLevel = args[1];
                int cacheSize = Integer.parseInt(args[2]);
                String strategy = args[3];

                new KVServer(port, logLevel, cacheSize, strategy).start();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }
}

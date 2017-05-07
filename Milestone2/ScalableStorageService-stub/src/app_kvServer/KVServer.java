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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class KVServer extends Thread
{

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private ServerSocket serverSocket;
    private ServerInfo sInfo;

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
        sInfo = new ServerInfo();
        setLogLevel(logLevel);

        if(cacheSize > 0) {
            sInfo.setCache(setCacheType(cacheSize, strategy));
        } else {
            sInfo.setCache(null);
            logger.error("Error! No caching enabled");
        }

        sInfo.setKvDirPath(Paths.get(String.valueOf(port)));
        sInfo.setmDataFile(Paths.get(sInfo.getKvDirPath().getFileName().toString() +
                File.separator + "metadata.txt"));
        try {
            createStorageObjects();
        } catch (IOException ex) {
            logger.error("Error! Unable to create directory /" + port);
            sInfo.setRunning(false);//terminate KVServer
        }
}

    private KVCache setCacheType(int cSize, String strat)
    {
        KVCache temp;
        if(strat.equals("FIFO")) {
            temp = new FIFOCache(cSize);
        } else if(strat.equals("LFU")) {
            temp = new LFUCache(cSize);
        } else if(strat.equals("LRU")) {
            temp = new LRUCache(cSize);
        } else {
            logger.error("Error! No caching enabled");
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

    /**
     * Creates a directory to store KV-pairs and a file
     * to store metadata
     * @throws IOException unable create such directory
     */
    public void createStorageObjects() throws IOException
    {
        if (Files.notExists(sInfo.getKvDirPath()))
        {
            Files.createDirectory(sInfo.getKvDirPath());

            logger.info("KVServer on port <" + this.port + "> " + "New Directory: "
                    + System.getProperty("user.dir") + File.separator +
                    sInfo.getKvDirPath().toString());
        }

        if (Files.notExists(sInfo.getmDataFile()))
        {
            Files.createFile(sInfo.getmDataFile());

            logger.info("KVServer on port <" + this.port + "> " + "Metadata file Path: "
                    + System.getProperty("user.dir") + File.separator +
                    sInfo.getmDataFile().toString());
        }
    }

    /**
     * If there is a metadata file that already contains start & end
     * hash ranges, then they are read when the first KVClient or ECS
     * connection is made. This happens when KVServer is closed and
     * reopened without clearing metadata file.
     * @param address   KVServer address
     * @param port      KVServer port
     * @throws IOException  erro in reading metadata file
     */
    public void setHashRange(String address, int port) throws IOException
    {
        ArrayList<String> metaData = new ArrayList<>(Files.readAllLines(sInfo.getmDataFile(),
                StandardCharsets.UTF_8));

        for(int i = 0; i < metaData.size(); i++)
        {
            String[] components = metaData.get(i).split(";");
            if(components[0].equals(address) && Integer.parseInt(components[1]) == port)
            {
                sInfo.setStartHash(components[2]);
                sInfo.setEndHash(components[3]);
                logger.error("startHash & endHash set");
                break;
            }
        }
    }

    private boolean isRunning()
    {
        return sInfo.getRunning();
    }

    public void run()
    {
        sInfo.setRunning(initializeServer());
        if(serverSocket != null)
        {
            while(isRunning())
            {
                try
                {
                    Socket client = serverSocket.accept();
                    setHashRange(client.getInetAddress().getHostAddress(),client.getLocalPort());

                    ClientConnection connection = new ClientConnection(client, sInfo);
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
        logger.info("Server stopped.");
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
        sInfo.setRunning(false);
        logger.info("KVServer<" + serverSocket.getInetAddress().getHostAddress() + ":"
                    + serverSocket.getLocalPort() + "> SHUTTING DOWN!!");
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

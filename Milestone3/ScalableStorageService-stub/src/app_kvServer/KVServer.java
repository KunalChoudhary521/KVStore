package app_kvServer;

import cache.FIFOCache;
import cache.KVCache;
import cache.LFUCache;
import cache.LRUCache;
import common.Metadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;

public class KVServer extends Thread
{

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private ServerSocket serverSocket;
    private Path kvDirPath;
    private Path mDataFile;
    private KVCache cache;
    private String startHash, endHash;
    private Boolean running;
    private Metadata[] replicas;
    private static final int REPLICA_LIMIT = 2;//can be set to zero so that M3 = M2
    private TreeMap<BigInteger, Metadata> hashRing;//updated whenever updated Metadata is received

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public ServerSocket getServerSocket()
    {
        return serverSocket;
    }

    public Path getKvDirPath()
    {
        return kvDirPath;
    }

    public Path getmDataFile()
    {
        return mDataFile;
    }

    public KVCache getCache()
    {
        return cache;
    }

    public String getStartHash()
    {
        return startHash;
    }

    public void setStartHash(String startHash)
    {
        this.startHash = startHash;
    }

    public String getEndHash()
    {
        return endHash;
    }

    public void setEndHash(String endHash)
    {
        this.endHash = endHash;
    }

    public Boolean getRunning()
    {
        return running;
    }

    public void setRunning(Boolean running)
    {
        this.running = running;
    }

    public Metadata[] getReplicas()
    {
        return replicas;
    }

    public void setReplicas(Metadata replica, int index)
    {
        this.replicas[index] = replica;
    }

    public TreeMap<BigInteger, Metadata> getHashRing()
    {
        return hashRing;
    }


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

        this.cache = setCacheType(cacheSize, strategy);
        if(this.cache == null)
        {
            logger.error("Error! No caching enabled");
        }

        this.kvDirPath = Paths.get(String.valueOf(port));
        this.mDataFile = Paths.get(this.kvDirPath.getFileName().toString() +
                                        File.separator + "metadata.txt");

        replicas = new Metadata[REPLICA_LIMIT];
        hashRing = new TreeMap<>();

        try {
            createStorageObjects();
        } catch (IOException ex) {
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

    /**
     * Creates a directory to store KV-pairs and a file
     * to store metadata
     * @throws IOException unable create such directory
     */
    public void createStorageObjects() throws IOException
    {
        if (Files.notExists(this.kvDirPath))
        {
            Files.createDirectory(this.kvDirPath);

            logger.info("KVServer on port <" + this.port + "> " + "New Directory: "
                    + System.getProperty("user.dir") + File.separator +
                    this.kvDirPath.toString());
        }

        if (Files.notExists(this.mDataFile))
        {
            Files.createFile(this.mDataFile);

            logger.info("KVServer on port <" + this.port + "> " + "Metadata file Path: "
                    + System.getProperty("user.dir") + File.separator + this.mDataFile.toString());
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

                    ClientConnection connection = new ClientConnection(client, this);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            +  " on port " + client.getPort());
                } catch (SocketException e1) {
                    stopServer();
                } catch (IOException e2) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e2);
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
        this.running = false;
        try {
            serverSocket.close();
            Files.deleteIfExists(this.mDataFile);//remove server's metadata file
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

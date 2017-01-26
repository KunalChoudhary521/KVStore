package app_kvServer;

import cache.LRUCache;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer  {
	
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

	private int port;
	private int cacheSize;
	private String strategy;
	private boolean isRunning;
	private ServerSocket socket;
    private static Logger logger = Logger.getRootLogger();
    private String KVFileName;

    private LRUCache cache;

	public KVServer(int port, int cacheSize, String strategy, String KVFileName) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.cache = new LRUCache(100);
		this.KVFileName = KVFileName;
	}

	public void Run(){
        isRunning = InitializeServer();
        if(socket != null) {
            while(isRunning){
                try {
                    Socket client = socket.accept();
                    ClientConnection connection =
                            new ClientConnection(client,this, this.KVFileName);
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
	}

	public void Stop(){
        isRunning = false;
        try {
            socket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

	private boolean InitializeServer(){
        logger.info("Initialize server ...");
        try {
            socket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + socket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    public static void main(String[] args){
	    // todo validation
	    int port = Integer.parseInt(args[0]);
	    int cacheSize = Integer.parseInt(args[1]);
        String strategy = args[2];
        String KVFileName = args[3];

        try {
            new LogSetup("logs/server.log", Level.ALL);
        } catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        }

        KVServer server = new KVServer(port, cacheSize, strategy, KVFileName);
        server.Run();
    }

    public void addToCache(String k, String v)
    {
        this.cache.insertInCache(k,v);
    }

    public String findInCache(String k)
    {
        String val = this.cache.checkCache(k);
        return val;
    }
}

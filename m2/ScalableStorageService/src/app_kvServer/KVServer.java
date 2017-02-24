package app_kvServer;


import cache.FIFOCache;
import cache.KVCache;
import cache.LFUCache;
import cache.LRUCache;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
	private boolean log;
	public Metadata myMetadata;
	private ArrayList<Metadata> serverMetadata;

	private String KVFileLocation;

	private KVCache cache;

	public KVServer(int port, int cSize, String strat, String KVFLocation, boolean log) {
		this.port = port;
		this.cacheSize = cSize;
		this.strategy = strat;
		if(cSize <= 0) {//user wants no caching
			this.cache = null;
		}
		else {
			if (this.strategy.equals("FIFO")) {
				this.cache = new FIFOCache(this.cacheSize);
			}
			else if (this.strategy.equals("LFU")) {
				this.cache = new LFUCache(this.cacheSize);
			}
			else if (this.strategy.equals("LRU")) {
				this.cache = new LRUCache(this.cacheSize);
			}
			else {
				this.cache = null;
			}
		}
		this.KVFileLocation = KVFLocation;
		this.log = log;

		serverMetadata = new ArrayList<Metadata>();
		this.buildMetadata();
	}

	private void buildMetadata() {
		File file = new File(this.KVFileLocation+"\\metadata");
		try {
			if (!file.exists()) {
				logger.info("No metadata file!");
				throw new Exception("no metadata file");
			}
			FileInputStream in = new FileInputStream(file);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));

			String line = null;
			while((line = reader.readLine()) != null){
				List<String> items = Arrays.asList(line.split(","));
				serverMetadata.add(new Metadata(items.get(0), items.get(1), items.get(2), items.get(3)));
			}
			reader.close();
			in.close();
		} catch (Exception ex){
			logger.info(ex);
		}
	}

	public String getMetadata() {
		String metadata = serverMetadata.size()+"!";
		for(int i =0; i < serverMetadata.size(); i++){
			Metadata item = serverMetadata.get(i);
			metadata+= item.host + ":" + item.port +"," + item.startHash + "," + item.endHash +"!";
		}
		return metadata;
	}

	public KVCache getKvcache(){return this.cache;}

	/**
	 * Repeatedly listen for incoming connections, once a connection is recieved,
	 * pass it to a worker thread. The work done by a worker thread is defined in
	 * ClientConnection
	 * */
	public void Run(){

		isRunning = InitializeServer();
		if(socket != null) {
			while(isRunning){
				try {
					Socket client = socket.accept();
					ClientConnection connection =
							new ClientConnection(client,this, this.KVFileLocation, this.log);
					new Thread(connection).start();

					if(log) {
						logger.info("Connected to "
								+ client.getInetAddress().getHostName()
								+ " on port " + client.getPort());
					}
				} catch (IOException e) {
					if(log) {
						logger.error("Error! " +
								"Unable to establish connection. \n", e);
					}
				}
			}
		}
		if(log) {
			logger.info("Server stopped.");
		}
	}

	/**
	 * Stops the server
	 * */
	public void Stop(){
		isRunning = false;
		try {
			socket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);

		}
	}

	/**
	 * Initializes the server by trying to obtain a socket on the port
	 * */
	private boolean InitializeServer(){
		logger.info("Initialize server ...");

		try {
			InetAddress address = InetAddress.getByName("localhost");
			socket = new ServerSocket(port, 0, address);
			logger.info("Server listening on " + address.getHostName()
                    +":" +  socket.getLocalPort());
			for(int i =0; i < serverMetadata.size(); i++){
				Metadata md = serverMetadata.get(i);
				if(socket.getInetAddress().getHostName().equals(md.host)){
					if(port == Integer.parseInt(md.port)){
						myMetadata = md;
						break;
					}
				}
			}

			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * KVServer contains a global cache and provides an interface to threads
	 * for adding to a cache
	 * @param k the key
	 * @param v the value
	 * */
	public void addToCache(String k, String v)
	{

		if (this.cacheSize >0) {
			logger.debug("adding "+k+" : "+ v+"to cache");
			this.cache.insertInCache(k, v);
		}
	}

	/**
	 * KVServer contains a global cache and provides an interface to threads
	 * for finding something in the cache
	 * @param k the key
	 * @param log variable to toggle logging
	 * */
	public String findInCache(String k, boolean log)
	{
		if (this.cacheSize == 0)
			return null;

		String val = this.cache.checkCache(k,log);
		logger.debug("found " + val +"in cache");
		return val;
	}

	/**
	 * Entry point for KVServer
	 * Parses command line arguments and starts a KVServer
	 * @param args an array of string command line arguments
	 * */
	public static void main(String[] args){
		// todo validation
		int port = Integer.parseInt(args[0]);
		int cacheSize = Integer.parseInt(args[1]);
		String strategy = args[2];
		boolean shouldLog = Boolean.parseBoolean(args[3]);

		try {
			new LogSetup(System.getProperty("user.dir")+"/logs/server.log", Level.ALL);
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}

		String currPath = System.getProperty("user.dir");
		File file = new File(""+port);
		if(!file.exists()){
			file.mkdir();
			if(shouldLog){
				logger.info("Creating a directory to place all key-value pairs");
			}
		}

		String KVFileLocation = file.getAbsolutePath();

		KVServer server = new KVServer(port, cacheSize, strategy, KVFileLocation, shouldLog);
		server.Run();
	}


}

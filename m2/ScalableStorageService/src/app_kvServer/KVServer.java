package app_kvServer;


import app_kvEcs.md5;
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
import java.util.concurrent.locks.ReentrantLock;

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
	public boolean isStarted;
	private ServerSocket socket;
	private static Logger logger = Logger.getRootLogger();
	private boolean log;
	private String host;

	// metadata logic
	private Metadata myMetadata;
	private ArrayList<Metadata> serverMetadata;
	ReentrantLock metaDataLock;
	private boolean amIFirstServerInRing;

	// Shutdown logic
	private boolean shouldShutDown;

	private String KVFileLocation;

	private KVCache cache;
	public boolean isReadOnly;
	private ArrayList<Socket> socketArray;
	public KVServer(String host, int port, int cSize, String strat, boolean log) {
		this.host = host;
		this.port = port;
		this.cacheSize = cSize;
		this.strategy = strat;
		this.isReadOnly = true;
		this.isStarted = false;
		this.shouldShutDown = false;
		metaDataLock = new ReentrantLock();
		amIFirstServerInRing = false;

		String currPath = System.getProperty("user.dir");

		logger.info(currPath);

		File file = new File(""+port);

		logger.info("KVStore directory's abs path: " + file.getAbsolutePath());
		logger.info("Name of KVStore's directory " + file.getName());

		if(!file.exists()){
			file.mkdir();
			if(log){
				logger.info("Creating a directory to place all key-value pairs");
			}
			File metadataFile = new File(file.getAbsolutePath() +"\\metadata");
			if(metadataFile.exists() == false) {
				try {
					metadataFile.createNewFile();
				} catch (Exception ex) {

				}
			} else {

			}
		}

		String KVFileLocation = file.getAbsolutePath();

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
		this.KVFileLocation = KVFileLocation;

		this.log = log;

		serverMetadata = new ArrayList<Metadata>();

		this.socketArray = new ArrayList<>();
		this.buildMetadata();
	}

	private void buildMetadata() {
		File file = new File(this.KVFileLocation+"\\metadata");
		try {
			if (!file.exists()) {
				logger.info("No metadata file!");
				throw new Exception("no metadata file");
			}

			logger.info("metadata file exists, populate metadata");
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
			while(isRunning && !shouldShutDown){
				try {
					Socket client = socket.accept();
					socketArray.add(client);
					ClientConnection connection =
							new ClientConnection(client,this, this.KVFileLocation, this.log);
					new Thread(connection).start();

					if(log) {
						logger.info("Connected to "
								+ client.getInetAddress().getHostName()
								+ " on port " + client.getPort());
					}
				} catch (Exception e) {
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
			InetAddress address = InetAddress.getByName(host);
			socket = new ServerSocket(port, 0, address);
			logger.info("Server listening on " + address.toString()
                    +":" +  socket.getLocalPort());

			updateMetadata();

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
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		int cacheSize = Integer.parseInt(args[2]);
		String strategy = args[3];
		boolean shouldLog = Boolean.parseBoolean(args[4]);

		try {
			new LogSetup(System.getProperty("user.dir")+"/logs/server.log", Level.ALL);
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}



		KVServer server = new KVServer(host, port, cacheSize, strategy, shouldLog);
		server.Run();
	}

	public void closeSocket(){
		this.isRunning = false;
		try {
			this.socket.close();
		} catch (Exception ex){
			logger.info(ex);
		}
	}

	// METADATA LOGIC

	public ArrayList<Metadata> getAllMetadata(){
		return this.serverMetadata;
	}

	public void metadataLock(){
		this.metaDataLock.lock();
	}

	public void metadataUnlock(){
		this.metaDataLock.unlock();
	}

	public void updateMetadata(){
		String a = socket.getInetAddress().toString();
		metadataLock();
		Metadata firstServer = null;

		//find "this" KVServer's metadata and set it
		for(int i =0; i < serverMetadata.size(); i++){
			Metadata md = serverMetadata.get(i);

			if(a.contains(md.host)){
				if(port == Integer.parseInt(md.port)){
					myMetadata = md;
					break;
				}
			}
		}

		//can check if you are the first server in the hash ring
		if(myMetadata != null) {
            if (myMetadata.startHash.compareTo(myMetadata.endHash) > 0) {
                amIFirstServerInRing = true;
            }
        }

        //since the ECS is the only thing that triggers a metadata update, write new metadata to file
		File file = new File(KVFileLocation+"\\metadata");
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
			for(int i=0; i < serverMetadata.size(); i++){
				Metadata m = serverMetadata.get(i);
				InetAddress address = InetAddress.getByName(m.host);
				String line = address.getHostAddress()+","+m.port+","+m.startHash+","+m.endHash+"\n";
				writer.write(line);
				writer.flush();
			}
			writer.close();
		} catch (Exception ex){
			logger.info(ex);
		}
		metadataUnlock();
	}

	public void takeNewMetadata(ArrayList<Metadata> newMetadata) {
		metadataLock();
		this.serverMetadata = newMetadata;
		updateMetadata();
		metadataUnlock();
	}

	public boolean amIResponsibleForThisHash(String hash){
		boolean foundMatch = false;

		this.metadataLock();
		for(int i =0 ; i < serverMetadata.size(); i++){
			Metadata curr = serverMetadata.get(i);
			int hashIsGreaterThanStart = hash.compareTo(curr.startHash);
			int hashIsLessThanEnd = hash.compareTo(curr.endHash);
			if(hashIsGreaterThanStart > 0 &&  hashIsLessThanEnd <=0){
				if(myMetadata.host.equals(curr.host) && myMetadata.port.equals(curr.port)){
					this.metadataUnlock();
					return true;
				} else {
					this.metadataUnlock();
					return false;
				}
			}
		}
		this.metadataUnlock();

		if(foundMatch == false){
			if(amIFirstServerInRing){
				return true;
			}
		}

		return false;
	}

	public void closeAllSockets()
	{
		for(int i = 0; i < socketArray.size(); i++)
        {
            try
            {
                socketArray.get(i).close();
            }
            catch (Exception ex)
            {
                logger.info(ex);
            }
        }

        try
        {
            this.socket.close();
        }
        catch (Exception ex)
        {
            logger.info(ex);
        }
	}

	public void Shutdown(boolean shutDown)
    {
        this.shouldShutDown = shutDown;
    }

}

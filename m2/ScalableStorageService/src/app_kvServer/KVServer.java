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
import java.math.BigInteger;
import java.net.*;
import java.util.*;
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
	private int port_r1;
	private int port_r2;
	private String host_r1;
	private String host_r2;

	// metadata logic
	private Metadata myMetadata;
	private TreeMap<BigInteger, Metadata> serverMetadata;//private ArrayList<Metadata> serverMetadata;
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
			File metadataFile = new File(file.getAbsolutePath() +"/metadata");
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

		serverMetadata=new TreeMap<>();//serverMetadata = new ArrayList<Metadata>();

		this.socketArray = new ArrayList<>();
		this.buildMetadata();
	}

	public int getPort()
	{
		return this.port;
	}
	private void buildMetadata() {
		File file = new File(this.KVFileLocation+"/metadata");
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
				serverMetadata.put(new BigInteger(items.get(4),16),new Metadata(items.get(0), items.get(1), items.get(2), items.get(3), items.get(4)));//serverMetadata.add(new Metadata(items.get(0), items.get(1), items.get(2), items.get(3), items.get(4)));
			}
			reader.close();
			in.close();
			update_replicas();
		} catch (Exception ex){
			logger.info(ex);
		}
	}

	public String getMetadata() {
		Collection <Metadata> met_col = serverMetadata.values();
		String metadata = met_col.size()+"!";
		for(Metadata item :met_col){
			//Metadata item = serverMetadata.get(i);
			metadata+= item.host + ":" + item.port +"," + item.startHash_g + "," + item.startHash_p+","+item.endHash +"!";
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
					if(log && !shouldShutDown) {
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
	 * Initializes the server by trying to obtain a socket on the port
	 * */
	private boolean InitializeServer(){
		logger.info("Initialize server ...");

		try {
			System.setProperty("sun.net.useExclusiveBind", "false");
			InetSocketAddress address = new InetSocketAddress(host, port);
			socket = new ServerSocket();
			socket.setReuseAddress(true);
			socket.bind(address,0);//new ServerSocket(port, 0, address);
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
			new LogSetup(System.getProperty("user.dir")+"/logs/server/server.log", Level.ALL);
		} catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}



		KVServer server = new KVServer(host, port, cacheSize, strategy, shouldLog);
		server.Run();
	}

	// METADATA LOGIC

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
		Collection<Metadata> met_col =serverMetadata.values();
		for(Metadata md: met_col){
			///Metadata md = serverMetadata.get(i);

			if(a.contains(md.host)){
				if(port == Integer.parseInt(md.port)){
					myMetadata = md;
					break;
				}
			}
		}

		//can check if you are the first server in the hash ring
		if(myMetadata != null) {
            if (myMetadata.startHash_p.compareTo(myMetadata.endHash) > 0) {
                amIFirstServerInRing = true;
            }
        }

        //since the ECS is the only thing that triggers a metadata update, write new metadata to file
		File file = new File(KVFileLocation+"/metadata");
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
			for(Metadata m: met_col){//int i=0; i < serverMetadata.size(); i++){
				//Metadata m = serverMetadata.get(i);
				InetAddress address = InetAddress.getByName(m.host);
				String line = address.getHostAddress()+","+m.port+","+m.startHash_g+","+m.startHash_p+","+m.endHash+"\n";
				writer.write(line);
				writer.flush();
			}
			writer.close();
		} catch (Exception ex){
			logger.info(ex);
		}
		metadataUnlock();
	}

	public void takeNewMetadata(TreeMap<BigInteger,Metadata> newMetadata) {
		metadataLock();
		this.serverMetadata = newMetadata;
		updateMetadata();
		metadataUnlock();
		update_replicas();
	}

	private void update_replicas(){
		BigInteger me = new BigInteger(this.myMetadata.endHash);
		Metadata rep_1;
		BigInteger rep_1k;
		Metadata rep_2;
		if (this.serverMetadata.lastKey() == me){
			rep_1 = this.serverMetadata.firstEntry().getValue();
			rep_1k=this.serverMetadata.firstKey();
		}else{
			rep_1 = this.serverMetadata.higherEntry(me).getValue();
			rep_1k = this.serverMetadata.higherKey(me);
		}
		if (this.serverMetadata.lowerKey(this.serverMetadata.lastKey())== me){
			rep_2 = this.serverMetadata.firstEntry().getValue();
		}
		else{
			rep_2 = this.serverMetadata.higherEntry(rep_1k).getValue();
		}
		this.port_r1 = Integer.parseInt(rep_1.port);
		this.port_r2 = Integer.parseInt(rep_2.port);
		this.host_r1 = rep_1.host;
		this.host_r2 = rep_2.host;
	}

	public boolean isResponsible(String clientHash, String caller)
    {
        String myStartHash;
        String myEndHash = this.myMetadata.endHash;
        if (caller.equalsIgnoreCase("get")){myStartHash= this.myMetadata.startHash_g;}
        else{myStartHash= this.myMetadata.startHash_p;}
        String largestHash = new String(new char[32]).replace("\0", "f");//ffff...[32]
        String smallestHash = new String(new char[32]).replace("\0", "0");//0000...[32]

        boolean result = false;

        if (myStartHash.compareTo(myEndHash) < 0)//start < end
        {
            if((clientHash.compareTo(myStartHash) >= 0) && (clientHash.compareTo(myEndHash) <= 0))
            {
                result = true;
            }
        }
        else if(myStartHash.compareTo(myEndHash) > 0)//start > end
        {
            /* This server is the 1st in the ring.
               Wrap-around case: start <= clientHash <= fff... || 000... <= clientHash <= end
             */
            if(((clientHash.compareTo(myStartHash) >= 0) && (clientHash.compareTo(largestHash) <= 0))
                    || ((clientHash.compareTo(smallestHash) >= 0) && (clientHash.compareTo(myEndHash) <= 0)))
            {
                result = true;
            }
        }
        else//start & end hash should never be equal to each other
        {
            logger.error("isResponsible :::: Start & End hash are equal!!\n");
        }

		return result;
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

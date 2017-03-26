package app_kvEcs;

import app_kvServer.Metadata;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class ECS implements ECSInterface {

    private static Logger logger = Logger.getRootLogger();
    private TreeMap<BigInteger, Metadata> hashRing;
    private Socket ecsSocket;
    private String configFile;
    private boolean log;
    private String currentMetaData;//only updated by updatedMetadata()
    private ArrayList<String> runningServers;//<IP:Port>
	  private ServerSocket failureSocket;
	  private FailureListener failureHandler;

    public ECS(boolean log)
    {
        this.configFile = "ecs.config";//ECS shouldn't be standalone (Piazza =193)
        this.log = log;
        hashRing = new TreeMap<>();
        runningServers = new ArrayList<>();
		
        failureHandler = new FailureListener(this, 8000, logger);
        new Thread(failureHandler).start();
    }
	
    public static void main(String[] args) {
        try {
            new LogSetup(System.getProperty("user.dir") + "/logs/ecs/ecs.log", Level.ALL);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

        TransferAndRemoveTest();
    }

    public static void TransferAndRemoveTest() {
        ECS ecs = new ECS(true);
        //logger.error("H1");

        //Reset file ecs.config (make all NA --> A)
        try {
            ecs.initKVServer(1, 10, "LRU", true);
            ecs.start();
            ecs.unlockWrite("127.0.0.1", 8080);
            //run KVClient and put key1,2,3,6
            //run KVServer2 @ port 8370
            //ecs.addNode(10,"LRU");

            //ecs.removeNode("127.0.0.1", 8080);//removeNode also shuts down this server

            ecs.shutDownKVServer("127.0.0.1", 8080);
            //ecs.shutDownKVServer("127.0.0.1", 8081);
            System.out.println("Test Done");
        } catch (Exception ex) {
            logger.error(ex);
        }
    }

    public ArrayList<String> getRunningServers()
    {
        return this.runningServers;
    }

    @Override
    public void initService(int numOfServers, int cSize, String strat) throws Exception
    {
        //logging is set to true in ECSClient.java
        if (numOfServers < 3) {
            throw new Exception("numOfServers must be greater than 2");
        }
        String line  = ""+numOfServers +", "+cSize+", "+strat;
        logger.info(line);
        initKVServer(numOfServers,cSize,strat,this.log);
    }

    public void initKVServer(int numOfServers, int cSize, String strat, boolean log) throws Exception
    {
        //System.out.println("Current Directory: " +  System.getProperty("user.dir"));
        if(numOfServers <= 2)
        {
            logger.error("initKVServer:: Invalid Number of Servers: " + numOfServers);
            return;
        }

        File file = new File(System.getProperty("user.dir")+"/"+this.configFile);

        logger.info(System.getProperty("user.dir"));

        String line;
        int serversRan = 0;

        try
        {
            BufferedReader rdBuffer = new BufferedReader(new FileReader(file));
            while(((line = rdBuffer.readLine()) != null) && (serversRan < numOfServers))//only read numOfServers lines
            {
                String[] address = line.split(",");

                if(address[2].equals("A"))//server is available/not in use
                {
                    runningServers.add(address[0] + ":" + address[1]);//used to update ecs config file

                    addToRing(address[0], Integer.parseInt(address[1]));
                    serversRan++;
                }
            }

            rdBuffer.close();
        }
        catch (Exception ex)
        {
            logger.error("ecs.config not in ScalableStorageServer directory. Create one Please.\n" +
                    "file format:<IP>,<port>,<A or NA>\n" +
                    "127.0.0.1,9000,A\n" +
                    "127.0.0.1,9002");
            throw ex;
        }


        //run all servers in stopped state
        Metadata temp;
        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            sshServer(entry.getValue().host,Integer.parseInt(entry.getValue().port),cSize,strat,this.log);//start
            // via SSH
            
            try{
              Thread.sleep(3000);
            } catch(Exception ex){
              logger.error("addNode: sleep-exception");
            }
            
            temp = entry.getValue();
            //runLocalServer(temp.host,Integer.parseInt(temp.port),cSize,strat);//testing locally
            stopKVServer(temp.host,Integer.parseInt(temp.port));//send stop message(disallow get & put)
        }

        updatedMetadata();

        sendMetadataToAll();//send metadata to all KVServers

        updateConfigFile();//edit config file (mark servers that are running)--> <IP>   <Port>  <NA>
    }

    private void updateConfigFile()
    {
        StringBuilder updatedFile = new StringBuilder();
        String[] params;
        BufferedReader reader = null;
        try
        {
            //read the whole file in memory
            reader = new BufferedReader(new FileReader(this.configFile));
            String line;
            while((line = reader.readLine()) != null)
            {
                params = line.split(",");
                //edit those strings whose IP:Port is in runningServers
                if(runningServers.contains(params[0] + ":" + params[1]))
                {
                    updatedFile.append(params[0] + "," + params[1] + "," + "NA" + "\n");//NA = non-available
                }
                else
                {
                    updatedFile.append(params[0] + "," + params[1] + "," + "A" + "\n");//A = available
                }
            }
            reader.close();

            //clear ecs.config and write updated data
            FileWriter confFile = new FileWriter(this.configFile,false);//false clears old content in the file
            confFile.write(updatedFile.toString());
            confFile.close();
        }
        catch(Exception ex)
        {
            logger.error("updateConfigFile:: ecs.config file not found");
            //ex.printStackTrace();
        }
    }

    private void updatedMetadata()
    {
        //Protocol: ECS-METADATA-<IP_1>,<P_1>,<sR_1>,<eR_1>-...-<IP_N>,<P_N>,<sR_N>,<eR_N>0
        String msg = "";
        int index = 1;

        //create byte array of all metadata
        for (Map.Entry<BigInteger, Metadata> entry : hashRing.entrySet()) {
            String host = entry.getValue().host;
            String port = entry.getValue().port;
            String srtRange_g = entry.getValue().startHash_g;
            String srtRange_p = entry.getValue().startHash_p;
            String endRange = entry.getValue().endHash;

            if (index != hashRing.size()) {
                msg += host + "," + port + "," + srtRange_g + "," + srtRange_p+ ","+ endRange + "-";
            }
            else {
                msg += host + "," + port + "," + srtRange_g + "," + srtRange_p+ "," +endRange;
            }
            index++;
            //System.out.println("MetaData byte array: " + new String(mData));//for debugging
        }

        currentMetaData = msg;
    }

    //Sends metadata to all servers
    public void sendMetadataToAll()
    {
        //Send Metadata contents to each running KVServer
        String preface = "ECS-METADATA-";
        String msg = preface + this.currentMetaData;
        byte[] mData = new byte[msg.length() + 1];
        System.arraycopy(msg.getBytes(),0,mData,0,msg.length());

        for(Map.Entry<BigInteger, Metadata> entry: hashRing.entrySet())
        {
            System.out.println(entry.getValue().host +", "+entry.getValue().port);
            sendViaTCP(entry.getValue().host,Integer.parseInt(entry.getValue().port),mData);
        }

        return;//for debugging
    }

    public void sendMetadata(String serverIP, int serverPort)
    {
        //Send Metadata contents to each running KVServer
        String preface = "ECS-METADATA-";
        String msg = preface + this.currentMetaData;
        byte[] mData = new byte[msg.length() + 1];
        System.arraycopy(msg.getBytes(),0,mData,0,msg.length());

        sendViaTCP(serverIP,serverPort,mData);

        return;//for debugging
    }

    private void sshServer(String ServerHost, int ServerPort, int cacheSize, String strategy, boolean log)
    {
        sshSession mySsh = new sshSession();
        String user = "milwidya", sshHost = "ug180.eecg.toronto.edu";//128.100.13.<>
        int sshPort = 22;

        mySsh.connectSsh(user,sshHost,sshPort);

        String cmd = "java -jar ms2-server.jar "+ ServerHost + " " + ServerPort +
                    " "+cacheSize +" " + strategy  + " " + log;

        String directory = "cd ece419/m2/ScalableStorageService";

        mySsh.runServer(directory, cmd);

        mySsh.session.disconnect();//can be moved to destructor
    }

    public void addToRing(String host, int port)
    {
        Metadata temp = new Metadata(host, Integer.toString(port));
        BigInteger currEndHash;
        Metadata nextServer, next_nextServer, next_next_nextServer;

        try
        {
            currEndHash =  md5.HashBI(host + ":" + port);
            temp.endHash = currEndHash.toString(16);


            if(hashRing.isEmpty())
            {
                temp.startHash_g = currEndHash.add(new BigInteger("1",10)).toString(16);
                temp.startHash_p = temp.startHash_g;
            }
            else if (hashRing.size()==1){
                if (hashRing.higherKey(currEndHash) == null){
                    nextServer = hashRing.firstEntry().getValue();

                    temp.startHash_g = currEndHash.add(new BigInteger("1", 10)).toString(16);
                    temp.startHash_p = (new BigInteger(nextServer.endHash.getBytes()).add(new BigInteger("1", 10))).toString(16);

                    nextServer.startHash_p=currEndHash.add(new BigInteger("1",10)).toString(16);


                }
                else{
                    nextServer = hashRing.higherEntry(currEndHash).getValue();

                    temp.startHash_g = currEndHash.add(new BigInteger("1", 10)).toString(16);
                    temp.startHash_p = (new BigInteger(nextServer.endHash.getBytes()).add(new BigInteger("1", 10))).toString(16);

                    nextServer.startHash_p = currEndHash.add(new BigInteger("1",10)).toString(16);

                }
            }
            else if(hashRing.size() ==2){
                if(hashRing.higherKey(currEndHash) == null)//no hash higher than currEndHash
                {
                    nextServer = hashRing.firstEntry().getValue();
                    next_nextServer=hashRing.higherEntry(hashRing.firstKey()).getValue();

                    temp.startHash_g = currEndHash.add(new BigInteger("1", 10)).toString(16);
                    temp.startHash_p = (new BigInteger(next_nextServer.endHash.getBytes()).add(new BigInteger("1", 10))).toString(16);

                    nextServer.startHash_p=currEndHash.add(new BigInteger("1",10)).toString(16);


                }
                else//server in the first position or somewhere in the middle
                {
                    nextServer = hashRing.higherEntry(currEndHash).getValue();
                    try{
                        next_nextServer = hashRing.higherEntry(hashRing.higherKey(currEndHash)).getValue();
                    } catch (Exception ex) {
                        next_nextServer = hashRing.lowerEntry(currEndHash).getValue();
                    }
                    temp.startHash_g = currEndHash.add(new BigInteger("1", 10)).toString(16);
                    temp.startHash_p = (new BigInteger(next_nextServer.endHash.getBytes()).add(new BigInteger("1", 10))).toString(16);

                    nextServer.startHash_p = currEndHash.add(new BigInteger("1", 10)).toString(16);

                }
            }
            else if(hashRing.containsKey(currEndHash))
            {
                return;//don't change the ring
            }
            else if(hashRing.higherKey(currEndHash) == null)//no hash higher than currEndHash
            {
                nextServer = hashRing.firstEntry().getValue();
                next_nextServer=hashRing.higherEntry(hashRing.firstKey()).getValue();
                next_next_nextServer=hashRing.higherEntry(hashRing.higherKey(hashRing.firstKey())).getValue();

                temp.startHash_g = nextServer.startHash_g;
                temp.startHash_p = nextServer.startHash_p;

                nextServer.startHash_p=currEndHash.add(new BigInteger("1",10)).toString(16);
                nextServer.startHash_g=next_nextServer.startHash_g;
                next_nextServer.startHash_g=temp.startHash_p;
                next_next_nextServer.startHash_g = nextServer.startHash_p;



            }
            else//server in the first position or somewhere in the middle
            {
                nextServer = hashRing.higherEntry(currEndHash).getValue();
                next_nextServer= hashRing.higherEntry(hashRing.higherKey(currEndHash)).getValue();
                next_next_nextServer=hashRing.higherEntry(hashRing.higherKey(hashRing.higherKey(currEndHash))).getValue();

                temp.startHash_g = nextServer.startHash_g;
                temp.startHash_p = nextServer.startHash_p;

                nextServer.startHash_p = currEndHash.add(new BigInteger("1",10)).toString(16);
                nextServer.startHash_g=next_nextServer.startHash_g;
                next_nextServer.startHash_g=temp.startHash_p;
                next_next_nextServer.startHash_g = nextServer.startHash_p;
            }

            hashRing.put(currEndHash, temp);// put into the hash ring
        }
        catch (Exception ex)
        {
            logger.error("addToRing:: Failed to add Server" + host + ":" + port + " to the Ring");
            ex.printStackTrace();
        }
    }

    @Override
    public String addNode(int cacheSize, String strategy)
    {
        //read from ecs.config and choose a server to run
        String newServerIP = null;
        int newServerPort = -1;
        File file = new File(System.getProperty("user.dir")+"/"+this.configFile);
        String line;
        try
        {
            BufferedReader rdBuffer = new BufferedReader(new FileReader(file));
            //read ecs.config until you have found a server to run
            while(((line = rdBuffer.readLine()) != null))
            {
                String[] address = line.split(",");

                if(address[2].equals("A"))//server is available/not in use
                {
                    newServerIP = address[0];
                    newServerPort = Integer.parseInt(address[1]);
                    runningServers.add(newServerIP + ":" + newServerPort);//to update ecs config file

                    //server added to Consistent Hash Ring (RB-Tree)
                    addToRing(newServerIP, newServerPort);

                    break;//no need to run anymore servers
                }
            }

            rdBuffer.close();
        }
        catch (Exception ex)
        {
            logger.error("addNode:: Failed to read ecs.config file");
            // ex.printStackTrace();
        }

        if((newServerIP == null) || (newServerPort < 0))//unable to find an available server to run
        {
            return null;
        }      

        sshServer(newServerIP, newServerPort, cacheSize, strategy,log);//start via SSH
        //runLocalServer(newServerIP, newServerPort, cacheSize, strategy);//for testing

        try{
          Thread.sleep(3000);
        } catch(Exception ex){
          logger.error("addNode: sleep-exception");
        }
        
        startKVServer(newServerIP,newServerPort);//send start message(allow get & put)

        lockWrite(newServerIP, newServerPort);

        updatedMetadata();
        sendMetadata(newServerIP,newServerPort);//send updated Metadata only to new added server

        if(hashRing.size() == 1)
        {
            //newly added server is the only one. No need to move any KV-pairs
            return null;
        }

        Metadata dstServer = null, srcServer = null;
        try
        {
            BigInteger newServerHash  =  md5.HashBI(newServerIP+":"+newServerPort);
            if(hashRing.higherEntry(newServerHash) == null)//new Server is last in the ring
            {
                srcServer = hashRing.firstEntry().getValue();//successor is first server in the ring
            }
            else
            {
                srcServer = hashRing.higherEntry(newServerHash).getValue();
            }

            dstServer = hashRing.get(newServerHash);

            //set write-lock on succesor
            lockWrite(srcServer.host, Integer.parseInt(srcServer.port));

            moveData(srcServer,dstServer, dstServer.startHash_g,dstServer.endHash);
        }
        catch (Exception ex)
        {
            logger.error("addNode:: Failed to find MD5 hash of: " + newServerIP + ":" + newServerPort);
            //ex.printStackTrace();
        }

        sendMetadataToAll();

        //runningServers.add(newServerIP + ":" + newServerPort);
        updateConfigFile();//mark servers as running

        return newServerIP+":"+newServerPort;
    }

    public void removeFromRing(String host, int port)
    {
        BigInteger currEndHash;

        try
        {
            currEndHash =  md5.HashBI(host + ":" + port);

            if(hashRing.isEmpty() || !hashRing.containsKey(currEndHash))
            {
                logger.info("removeFromRing:: Server " + host + ":" + port + " does not exist."
                            + "Nothing removed from the Ring");
                return;
            }
            else if(hashRing.higherKey(currEndHash) == null)//no server hash higher than currEndHash
            {
                hashRing.higherEntry(hashRing.firstKey()).getValue().startHash_g = hashRing.firstEntry().getValue().startHash_g;
                hashRing.firstEntry().getValue().startHash_p = hashRing.get(currEndHash).startHash_p;
                hashRing.firstEntry().getValue().startHash_g = hashRing.get(currEndHash).startHash_g;
            }
            else //server in the first position or somewhere in the middle
            {
                hashRing.higherEntry(hashRing.higherKey(currEndHash)).getValue().startHash_g = hashRing.higherEntry((currEndHash)).getValue().startHash_g;
                hashRing.higherEntry(currEndHash).getValue().startHash_p = hashRing.get(currEndHash).startHash_p;
                hashRing.higherEntry(currEndHash).getValue().startHash_g = hashRing.get(currEndHash).startHash_g;
            }

            hashRing.remove(currEndHash);
        }
        catch (Exception ex)
        {
            logger.error("removeFromRing:: Failed to remove Server " + host + ":" + port + " from the Ring");
            //ex.printStackTrace();
        }
    }

    @Override
    public void removeNode(String hostToRmv, int portToRmv) throws Exception//removing should not be random
    {
        if(runningServers.size() < 4)
        {
            logger.info("removeNode:: Only 3 servers currently running, cannot remove");
            throw new Exception("removeNode:: Only 3 servers currently running, cannot remove");

        }
        /*else if(runningServers.size() == 1)
        {
            //Delete its directory (or only KVPairs & metadata)
            logger.info("removeNode:: Only 1 sever is currently running");
            return;
        }*/
        Metadata serverToRmv = null;
        Metadata succServer = null;

        try
        {
            //find successor server
            BigInteger serverToRmvHash = md5.HashBI(hostToRmv + ":" + portToRmv);
            Metadata temp = hashRing.get(serverToRmvHash);
            serverToRmv = new Metadata(temp.host,temp.port,temp.startHash_g,temp.startHash_p, temp.endHash);

            if(hashRing.higherEntry(serverToRmvHash) == null)
            {
                succServer = hashRing.firstEntry().getValue();//1st server in the ring
            }
            else
            {
                succServer = hashRing.higherEntry(serverToRmvHash).getValue();
            }

            removeFromRing(hostToRmv,portToRmv);
            updatedMetadata();

            //lockwrite server from where the KV-paris are being removed
            lockWrite(hostToRmv, portToRmv);

            sendMetadata(succServer.host, Integer.parseInt(succServer.port));

            moveData(serverToRmv,succServer,serverToRmv.startHash_g,serverToRmv.endHash);

            shutDownKVServer(serverToRmv.host,Integer.parseInt(serverToRmv.port));
        }
        catch (Exception ex)
        {
            logger.error("removeNode:: Could not find successor server of " + hostToRmv
                    + ":" + portToRmv);
        }

        sendMetadataToAll();

        runningServers.remove(hostToRmv + ":" + portToRmv);
        updateConfigFile();//mark removed server as available

    }
    
    public void handleFailure(String host, int port){
      try{
        logger.info("ECS: adding a new node");
        String target = addNode(100, "LRU");        
        logger.info("ECS: removing a failed node");
        removeNode(host, port);
        start();
        String[] params = target.split(":");
        String thost = params[0];
        String tport = params[1];
        unlockWrite(thost, Integer.parseInt(tport.trim()));
      } catch(Exception ex){
        logger.error("ECS: " + ex.getMessage());
      }
    }

    public void startKVServer(String host, int port)
    {
        //Protocol: ECS-START0
        byte[] byteMsg = createMessage("ECS-START");
        sendViaTCP(host, port, byteMsg);
    }

    /*Starts the storage service by calling
    *startKVServer() on all KVServer instances
    * that participate in the service.
     */
    @Override
    public void start()
    {
        Metadata temp;
        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            temp = entry.getValue();
            startKVServer(temp.host,Integer.parseInt(temp.port));//send start message(allow get & put)
        }
    }

    /*Stops all KVServers (don't allow get & put)
    * uses stopKVServer
     */
    @Override
    public void stop()
    {
        Metadata temp;
        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            temp = entry.getValue();
            stopKVServer(temp.host,Integer.parseInt(temp.port));//send stop message(dis-allow get & put)
        }
    }

    public void stopKVServer(String host, int port)
    {
        //Protocol: ECS-STOP0
        byte[] byteMsg = createMessage("ECS-STOP");
        sendViaTCP(host, port, byteMsg);
    }

    /*
        Shutdowns all running KVServers
     */
    @Override
    public void shutDown()
    {
        Metadata temp;
        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            temp = entry.getValue();
            shutDownKVServer(temp.host,Integer.parseInt(temp.port));//terminate all KVServer instances
        }
        updateConfigFile();//mark removed server as available        
    }

    public void shutDownKVServer(String host, int port)
    {
        //Protocol: ECS-SHUTDOWN0
        byte[] byteMsg = createMessage("ECS-SHUTDOWN");
        sendViaTCP(host, port, byteMsg);

        runningServers.remove(host + ":" + port);
    }

    @Override
    public void lockWrite(String host, int port)
    {
        //Protocol: ECS-LOCKWRITE0
        byte[] byteMsg = createMessage("ECS-LOCKWRITE");
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void unlockWrite(String host, int port)
    {
        //Protocol: ECS-UNLOCKWRITE0
        byte[] byteMsg = createMessage("ECS-UNLOCKWRITE");
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void moveData(Metadata srcServer, Metadata dstServer, String startRange, String endRange)
    {
        try
        {
            //srcServer & dstServer should be running by now
            //make sure receiver is ready to receiver KV-pairs

            String kvSender = "ECS-MOVE-KV-" +
                            dstServer.host + "-" + dstServer.port + "-" +
                            startRange + "-" + endRange + "-";


            byte[] senderMsg = createMessage(kvSender);
            sendViaTCP(srcServer.host, Integer.parseInt(srcServer.port), senderMsg);


            //wait for ACK from srcServer which signals that transfer is complete
            byte[] buffer = new byte[100];
            InputStream in = ecsSocket.getInputStream();

            int count;
            String ack;
            while((count = in.read(buffer)) >= 0)
            {
                ack = new String(buffer);//expect from Server: DONE
                //logger.info("KVServer ACK buffer: " + ack);
                if(ack.contains("DONE-TRANSFER"))//"FIN"
                {
                    logger.info("Transfer done by " + srcServer.host + ":" + srcServer.port);
                    break;
                }
            }
            //Doesn't handle transfer failures, yet (network failure)

            in.close();
            ecsSocket.close();

            //unlock write on successor server to re-allow put()
            unlockWrite(srcServer.host, Integer.parseInt(srcServer.port));
        }
        catch(Exception ex)
        {
            logger.error("moveData:: Failed to moveData to " + dstServer.host + ":" + dstServer.port);
            //ex.printStackTrace();
        }
    }

    public byte[] createMessage(String msgType)
    {
        byte[] byteMsg = new byte[msgType.length() + 1];
        System.arraycopy(msgType.getBytes(),0,byteMsg,0,msgType.length());
        //System.out.println("byte-message: " + new String(byteMsg));
        return byteMsg;
    }

    @Override
    /*
    * All TCP logic for sending the kvServer updated metadata
    *
     */
    public void sendViaTCP(String host, int port, byte[] data)
    {
        try {          
            this.ecsSocket = new Socket(host, port);//KVServer must be ready before ECS sends data
            OutputStream writeToSock = this.ecsSocket.getOutputStream();
            InputStream input = this.ecsSocket.getInputStream();

            writeToSock.write(data,0,data.length);
            writeToSock.flush();

            byte[] response = new byte[100];
            // wait for acknowledge message
            input.read(response);

            // disconnect
            String disconnect = "ECS-DISCONNECT";
            byte[] disconnectMsg = new byte[disconnect.length() + 1];//+1 for stream termination
            System.arraycopy(disconnect.getBytes(),0,disconnectMsg,0,disconnect.length());
            writeToSock.write(disconnectMsg, 0, disconnectMsg.length);

            //for ECS-mOVE-KV, socket is closed in moveData after receiving DONE-TRANSFER Signal
            if(!(new String(data)).contains("ECS-MOVE-KV"))
            {
                this.ecsSocket.close();
            }
        }
        catch (Exception ex)
        {
            logger.error("sendViaTCP:: ECS failed to send data to KVServer: " + host + ":" + port);
            ex.printStackTrace();
        }
    }
}

class FailureListener implements Runnable {
  ServerSocket failureSocket;
  private Logger logger;
  private ECS ecs;
	
	public FailureListener(ECS ecs, int port, Logger logger){
    this.ecs = ecs;
		try{
      InetSocketAddress address = new InetSocketAddress("localhost", port);
      failureSocket = new ServerSocket();
      failureSocket.bind(address, 0);
      this.logger = logger;
      this.logger.info("Listening for failures on " + address.toString() 
      + ":" + failureSocket.getLocalPort());      
    } catch(Exception ex){
      logger.error("FailureListener: " + ex.getMessage());
    }
     
	}
	
	public void run(){
	  try{
      while(true){
        try{          
          Socket primaryReplica = failureSocket.accept();          
          FailureHandler handler = new FailureHandler(ecs, primaryReplica, logger);          
					new Thread(handler).start();                    
        }catch(Exception e){
          logger.error("FailureListener: creating a failure handler" + e.getMessage());
        }
      }
	  } catch(Exception ex)  {
			logger.error("FailureListener: failure in the while loop" + ex.getMessage());      
	  }
	}

	public void CloseListener(){
    try{
      this.failureSocket.close();  
    } catch (Exception ex){
      logger.error("FailureListener: closing the listener " + ex.getMessage());
    }
	} 
}

class FailureHandler implements Runnable {
  Socket socket;
  private InputStream input;
  private OutputStream output;
  private Logger logger;
  private static final int BUFFER_SIZE = 1024;
  private static final int DROP_SIZE = 128 * BUFFER_SIZE;
  private ECS ecs;
  private String lastFailure;
  private String lastFailurePort;
  private boolean isStopped;
  
  public FailureHandler(ECS ecs, Socket socket, Logger logger){
    this.socket = socket;    
    this.logger = logger;
    this.ecs = ecs;
    this.lastFailure = null;
    this.lastFailurePort = null;
    this.isStopped = false;
    logger.info("FailureHandler: I was created to handle a failure...");
  }
  
  public void run(){
    TextMessage latestMsg = null;
    try{           
      input = socket.getInputStream();
      logger.info("FailureHandler: I am trying to receive a message...");
      latestMsg = receiveMessage();        
      logger.info("received: " + latestMsg.getMsg());
      this.isStopped = true;            
    } catch(Exception ex){
      logger.error("FailureHandler: " + ex.getMessage());
    }
  
    String[] replica = latestMsg.getMsg().split("-");
    String host = replica[1];
    String port = replica[2];
    
    logger.info("FailureHandler: " + host + ":" + port);
    logger.info("FailureHandler: " + lastFailure);
    
    if(this.lastFailure == null){
      this.lastFailure = host.toString();
      this.lastFailurePort = port.toString();
      ecs.handleFailure(host, Integer.parseInt(port.trim()));
    } else {
      if(!( host.equals(this.lastFailure)&&port.equals(this.lastFailurePort) ) ){
        ecs.handleFailure(host, Integer.parseInt(port.trim()));
        this.lastFailure = host;
        this.lastFailurePort = port;
      } else {
        logger.info("FailureHandler: Already handled the last failure!");
      }
    }
    
  }

  private TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 0 && reading && !isStopped) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 0 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
  }  
}

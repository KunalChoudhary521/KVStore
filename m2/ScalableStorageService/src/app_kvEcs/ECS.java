package app_kvEcs;

import app_kvServer.Metadata;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class ECS implements ECSInterface {

    private static Logger logger = Logger.getRootLogger();;
    private TreeMap<BigInteger, Metadata> hashRing;
    private Socket ecsSocket;
    private String configFile;
    private boolean log;
    private String currentMetaData;//only updated by updatedMetadata()
    private ArrayList<String> runningServers;//<IP:Port>

    public ECS(boolean log)
    {
        this.configFile = "ecs.config";//ECS shouldn't be standalone (Piazza =193)
        this.log = log;
        hashRing = new TreeMap<>();
        runningServers = new ArrayList<>();
    }

    @Override
    public void initService(int numOfServers, int cSize, String strat)
    {
        //logging is set to true in ECSClient.java
        initKVServer(numOfServers,cSize,strat,this.log);
    }

    public void initKVServer(int numOfServers, int cSize, String strat, boolean log)
    {
        //Currently, numOfServers is not used
        //System.out.println("Current Directory: " +  System.getProperty("user.dir"));
        if(numOfServers <= 0)
        {
            logger.error("initKVServer:: Invalid Number of Servers: " + numOfServers);
            return;
        }

        File file = new File(System.getProperty("user.dir")+"/"+this.configFile);

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
        }

        //run all servers in stopped state
        Metadata temp;
        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            //sshServer(entry.getValue(),cacheSize,strat,this.log);//start via SSH
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
            String srtRange = entry.getValue().startHash;
            String endRange = entry.getValue().endHash;

            if (index != hashRing.size()) {
                msg += host + "," + port + "," + srtRange + "," + endRange + "-";
            }
            else {
                msg += host + "," + port + "," + srtRange + "," + endRange;
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
        String user = "rahmanz5", sshHost = "ug222.eecg.toronto.edu";//128.100.13.<>
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
        Metadata nextServer, prevServer;

        try
        {
            currEndHash =  md5.HashBI(host + ":" + port);
            temp.endHash = currEndHash.toString(16);


            if(hashRing.isEmpty())
            {
                temp.startHash = currEndHash.add(new BigInteger("1",10)).toString(16);
            }
            else if(hashRing.containsKey(currEndHash))
            {
                return;//don't change the ring
            }
            else if(hashRing.higherKey(currEndHash) == null)//no hash higher than currEndHash
            {
                prevServer = hashRing.firstEntry().getValue();
                temp.startHash = prevServer.startHash;
                prevServer.startHash = currEndHash.add(new BigInteger("1",10)).toString(16);
            }
            else//server in the first position or somewhere in the middle
            {
                nextServer = hashRing.higherEntry(currEndHash).getValue();
                temp.startHash = nextServer.startHash;
                nextServer.startHash = currEndHash.add(new BigInteger("1",10)).toString(16);
            }

            hashRing.put(currEndHash, temp);// put into the hash ring
        }
        catch (Exception ex)
        {
            logger.error("addToRing:: Failed to add Server" + host + ":" + port + " to the Ring");
            //ex.printStackTrace();
        }
    }

    @Override
    public void addNode(int cacheSize, String strategy)
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
            return;
        }

        //sshServer(host, port, cacheSize, strategy,log);//start via SSH
        //runLocalServer(newServerIP, newServerPort, cacheSize, strategy);//for testing
        startKVServer(newServerIP,newServerPort);//send start message(allow get & put)

        lockWrite(newServerIP, newServerPort);

        updatedMetadata();
        sendMetadata(newServerIP,newServerPort);//send updated Metadata only to new added server

        if(hashRing.size() == 1)
        {
            //newly added server is the only one. No need to move any KV-pairs
            return;
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

            moveData(srcServer,dstServer, dstServer.startHash,dstServer.endHash);
        }
        catch (Exception ex)
        {
            logger.error("addNode:: Failed to find MD5 hash of: " + newServerIP + ":" + newServerPort);
            //ex.printStackTrace();
        }

        sendMetadataToAll();

        runningServers.add(newServerIP + ":" + newServerPort);
        updateConfigFile();//mark servers as running
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
                hashRing.firstEntry().getValue().startHash = hashRing.get(currEndHash).startHash;
            }
            else //server in the first position or somewhere in the middle
            {
                hashRing.higherEntry(currEndHash).getValue().startHash = hashRing.get(currEndHash).startHash;
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
    public void removeNode(String hostToRmv, int portToRmv)//removing should not be random
    {
        if(runningServers.size() == 0)
        {
            logger.info("removeNode:: No server is currently running");
            return;
        }
        //TODO: last server is removed
        else if(runningServers.size() == 1)
        {
            //Delete its directory (or only KVPairs & metadata)
            logger.info("removeNode:: Only 1 sever is currently running");
            return;
        }
        Metadata serverToRmv = null;
        Metadata succServer = null;

        try
        {
            //find successor server
            BigInteger serverToRmvHash = md5.HashBI(hostToRmv + ":" + portToRmv);
            Metadata temp = hashRing.get(serverToRmvHash);
            serverToRmv = new Metadata(temp.host,temp.port,temp.startHash, temp.endHash);

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

            moveData(serverToRmv,succServer,serverToRmv.startHash,serverToRmv.endHash);

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
    }


    public void shutDownKVServer(String host, int port)
    {
        //Protocol: ECS-SHUTDOWN0
        byte[] byteMsg = createMessage("ECS-SHUTDOWN");
        sendViaTCP(host, port, byteMsg);

        /*Should be done in stop()
        try {
            hashRing.remove(md5.HashS(host));
            sendUpdatedMetadata();
        } catch (Exception ex){
            ex.printStackTrace();
        }
        */
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

    public void runLocalServer(String host, int port, int cacheSize, String strategy)
    {
        String command = "java -jar ms2-server.jar " + host + " " + port +
                " " + cacheSize + " " + strategy + " " + this.log;
        try {
            Process cmdProc = Runtime.getRuntime().exec(command);
            //System.out.println("Current Directory: " +  System.getProperty("user.dir"));
            //cmdProc.destroy();
            System.out.println("Running: " + host + ":" + port + " " + cmdProc.isAlive());
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Failed to run server locally");
        }
    }

    public static void main(String[] args){
        try
        {
            new LogSetup(System.getProperty("user.dir")+"/logs/ecs/ecs.log", Level.ALL);
        }
        catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        ECS ecs = new ECS(true);
        //logger.error("H1");

        //Reset file ecs.config (make all NA --> A)
        ecs.initKVServer(1,10,"LRU",true);
        ecs.start();
        ecs.unlockWrite("127.0.0.1",9000);
        //run KVClient and put key1,2,3,6
        //run KVServer2 @ port 8370
        ecs.addNode(10,"LRU");

        ecs.removeNode("127.0.0.1", 9000);
    }
}

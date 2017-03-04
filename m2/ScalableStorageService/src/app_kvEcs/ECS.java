package app_kvEcs;

import app_kvServer.KVServer;
import app_kvServer.Metadata;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class ECS implements ECSInterface {

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
            return;
        }

        File file = new File(System.getProperty("user.dir")+"\\"+this.configFile);

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
            System.out.println("ecs config file not found");
            //logger error: ecs.config not in ScalableStorageServer directory. Create one Please.
            /*file format:<IP>,<port>,<A or NA>
              127.0.0.1,9000
              127.0.0.1,9002
            */
            ex.printStackTrace();
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
            System.out.println("ecs config file not found");
            ex.printStackTrace();
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
            ex.printStackTrace();
        }
    }

    @Override
    public void addNode(String newServerIP, int newServerPort, int cacheSize, String strategy)
    {
        //need to read from ecs.config and choose a server to run
        addToRing(newServerIP,newServerPort);

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

        Metadata dstServer = null;
        try
        {
            dstServer = hashRing.get(md5.HashBI(newServerIP+":"+newServerPort));
            moveData(dstServer, dstServer.startHash,dstServer.endHash);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
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
                //logger statement, nothing to remove
                return;
            }
            else if(hashRing.higherKey(currEndHash) == null)//no hash higher than currEndHash
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
            ex.printStackTrace();
        }

        //need to handle shutdown

        /*  Call sendUpdatedMetadata() outside this function b/c:
            -   There might have been multiple entries removed from hashRing.
            -   Wait for Server to start via SSH (might be slow due to SSH)
        */
    }

    @Override
    public void removeNode(String host, int port)
    {

        runningServers.remove(host + ":" + port);
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
        //todo
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
        //todo
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
    public void moveData(Metadata dstServer, String startRange, String endRange)
    {
        Metadata srcServer = null;
        //find successor/srcServer server
        try
        {
            BigInteger newServerHash  =  md5.HashBI(dstServer.host + ":" + dstServer.port);//server recently added
            if(hashRing.higherEntry(newServerHash) == null)//new Server is last in the ring
            {
                srcServer = hashRing.firstEntry().getValue();//successor is first server in the ring
            }
            else
            {
                srcServer = hashRing.higherEntry(newServerHash).getValue();
            }

            //srcServer & dstServer should be running by now
            String kvReceiver = "ECS-RECV-KV-";//send this to toServer
            byte[] recvMsg = createMessage(kvReceiver);
            sendViaTCP(dstServer.host, Integer.parseInt(dstServer.port), recvMsg);

            /*
            ECS might needs to wait until toServer is ready to
            receive KV-pairs before commanding fromServer to start sending.
            */

            //set write-lock on succesor
            lockWrite(srcServer.host, Integer.parseInt(srcServer.port));

            String kvSender = "ECS-SEND-KV-" +
                            dstServer.host + "-" + dstServer.port + "-" +
                            startRange + "-" + endRange + "-";


            byte[] senderMsg = createMessage(kvSender);
            sendViaTCP(srcServer.host, Integer.parseInt(srcServer.port), senderMsg);


            //wait for ACK from srcServer that transfer is complete
            ServerSocket recvSock = new ServerSocket(Integer.parseInt(srcServer.port));
            ecsSocket = recvSock.accept();
            byte[] buffer = new byte[15];
            InputStream in = ecsSocket.getInputStream();
            int count;
            String ack;
            while((count = in.read(buffer)) >= 0)
            {
                ack = new String(buffer);//expect from Server: DONE
                if(ack.equals("DONE"))
                {
                    break;
                }
            }
            //Doesn't handle transfer fails, yet (network failure)

            in.close();
            ecsSocket.close();

            //unlock write on successor server to re-allow put()
            unlockWrite(srcServer.host, Integer.parseInt(srcServer.port));
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
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
            this.ecsSocket = new Socket(host, port);//KVServer must be ready to accept TCP request before ECS sends data
            OutputStream writeToSock = this.ecsSocket.getOutputStream();
            InputStream input = this.ecsSocket.getInputStream();

            writeToSock.write(data,0,data.length);
            writeToSock.flush();

            byte[] response = new byte[1024];
            // wait for acknowledge message
            input.read(response);

            // disconnect
            byte[] disconnectMsg = {'E','C','S','-','D','I','S','C','O','N','N','E','C','T',0};
            writeToSock.write(disconnectMsg, 0, disconnectMsg.length);

            this.ecsSocket.close();
        }
        catch (Exception ex)
        {
            System.out.println("ECS failed to send data to KVServer: " + host + ":" + port);
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
        ECS ecs = new ECS(true);

        ecs.initKVServer(1,10,"LRU",true);
        ecs.start();
        //ecs.startKVServer("127.0.0.1", 8080);
    }
}

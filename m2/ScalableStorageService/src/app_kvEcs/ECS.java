package app_kvEcs;

import app_kvServer.Metadata;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;

public class ECS implements ECSInterface {

    private TreeMap<BigInteger, Metadata> hashRing;
    private Socket ecsSocket;
    private String fileName;

    public ECS(String filename){
        this.fileName = filename;
        hashRing = new TreeMap<>();
    }

    @Override
    public void initService(int numOfServers, int cSize, String strat, boolean log)
    {
        //Currently, numOfServers is not used
       // String fileName = "ecs.config";//needs to be command-line arg
        System.out.println("Current Directory: " +  System.getProperty("user.dir"));

        File file = new File(System.getProperty("user.dir")+"\\"+this.fileName);
        if(!file.exists())
        {
            //logger error: ecs.config not in ScalableStorageServer directory. Create one Please.
            /*file format:<IP>,<port>
              127.0.0.1,9000
              127.0.0.1,9002
            */
            return;
        }

        String line;
        try
        {
            BufferedReader rdBuffer = new BufferedReader(new FileReader(file));
            while((line = rdBuffer.readLine()) != null)
            {
                String[] ipAndPort = line.split(",");
                addServer(ipAndPort[0],Integer.parseInt(ipAndPort[1]),cSize,strat,log);//also starts server via SSH
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

        //Calls is made inside because all server have been added to the Metadata file
        //sendUpdatedMetadata();
    }
    private void sendUpdatedMetadata()
    {
        //Protocol: ECS-METADATA-<IP_1>,<P_1>,<sR_1>,<eR_1>-...-<IP_N>,<P_N>,<sR_N>,<eR_N>0
        String msg = "ECS-METADATA-";
        int index = 1;

        //create byte array of all metadata
        for(Map.Entry<BigInteger,Metadata> entry: hashRing.entrySet())
        {
            String host = entry.getValue().host;
            String port = entry.getValue().port;
            String srtRange = entry.getValue().startHash;
            String endRange = entry.getValue().endHash;

            if(index != hashRing.size()) {
                msg += host + "," + port + "," + srtRange + "," + endRange + "-";
            } else {
                msg += host + "," + port + "," + srtRange + "," + endRange;
            }
            index++;
            //System.out.println("MetaData byte array: " + new String(mData));//for debugging
        }

        //Send Metadata contents to each running KVServer
        byte[] mData = new byte[msg.length() + 1];
        System.arraycopy(msg.getBytes(),0,mData,0,msg.length());

        for(Map.Entry<BigInteger, Metadata> entry: hashRing.entrySet())
        {
            sendViaTCP(entry.getValue().host,Integer.parseInt(entry.getValue().port),mData);
        }
        return;//for debugging
    }

    private void sshServer(Metadata m, int cacheSize, String strategy, boolean log)
    {
        sshSession mySsh = new sshSession();
        String user = "rahmanz5", host = "ug222.eecg.toronto.edu";//128.100.13.<>
        int port = 22;

        mySsh.connectSsh(user,host,port);

        String cmd = "java -jar ms2-server.jar "+ m.host + " " + m.port +" "+cacheSize +" " + strategy  + " " + log;
        String directory = "cd ece419/m2/ScalableStorageService";

        mySsh.runServer(directory, cmd);

        mySsh.session.disconnect();//can be moved to destructor
    }

    @Override
    public void addServer(String host, int port, int cacheSize, String strategy, boolean log)
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
        //this.sshServer(m,cacheSize,strategy,log);//start server via SSH

        /*  Call sendUpdatedMetadata() outside this function because:
            -   There might have been multiple entries added to hashRing.
            -   Wait for Server to start via SSH (might be slow due to SSH)
        */
    }

    @Override
    public void removeServer(String host, int port)
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
    public void start(String host, int port)
    {
        //Protocol: ECS-START0
        byte[] byteMsg = createMessage("ECS-START");
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void stop(String host, int port)
    {
        //Protocol: ECS-STOP0
        byte[] byteMsg = createMessage("ECS-STOP");
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void shutDown(String host, int port)
    {
        //Protocol: ECS-SHUTDOWN0
        byte[] byteMsg = createMessage("ECS-SHUTDOWN");
        sendViaTCP(host, port, byteMsg);

        try {
            hashRing.remove(md5.HashS(host));
            sendUpdatedMetadata();
        } catch (Exception ex){
            ex.printStackTrace();
        }
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
    public void moveData(String host, int port, String startRange, String endRange)
    {
        //Protocol: ECS-MOVE-<start-range>-<end-range>-0
        String msgType = "ECS-MOVE-";

        byte[] byteMsg = new byte[msgType.length() + (1+startRange.length()) + (1+endRange.length()) + 2];//last +2 for 0 terminator

        System.arraycopy(msgType.getBytes(),0,byteMsg,0,msgType.length());

        int startRIdx = msgType.length()+1;
        byte[] startBytes = startRange.getBytes();
        System.arraycopy(startBytes,0,byteMsg,startRIdx,startBytes.length);
        byteMsg[startRIdx+startBytes.length] = '-';

        int endRIdx = (startRIdx+startBytes.length) + 1;
        byte[] endBytes = endRange.getBytes();
        System.arraycopy(endBytes,0,byteMsg,endRIdx,endBytes.length);
        byteMsg[byteMsg.length-2] = '-';

        //System.out.println("Byte Msg: " + new String(byteMsg));//for debugging

        sendViaTCP(host, port, byteMsg);
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

    @Override
    /*
    * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
    *calls addService
     */
    public void addNode(int cacheSize, String strategy)
    {
    //toDo
    }

    @Override
    /*
    * Remove a server from the storage service at an arbitrary position.
     */
    public void removeNode()
    {
        //toDo
    }

    public static void main(String[] args){
        ECS ecs = new ECS("ecs.config");

        // use this for testing on ug machines
        //Metadata fake1 = new Metadata("128.100.13.222", "8000", "asfdsaf", "fdasfddas");


        // use this for testing wrap around (create metadata file in the folder where the kvps are stored)
        Metadata fake1 = new Metadata("localhost", "8000", "f0000000000000000000000000000000", "c0000000000000000000000000000000");
        Metadata fake2 = new Metadata("localhost", "8001", "c0000000000000000000000000000001", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeef");

        // use this for testing on localhost
        //Metadata fake1 = new Metadata("localhost", "8000", "99999999999999999999999999999999", "33333333333333333333333333333332");
        //Metadata fake2 = new Metadata("localhost", "8001", "33333333333333333333333333333333", "55555555555555555555555555555554");
        //Metadata fake3 = new Metadata("localhost", "8002", "55555555555555555555555555555555", "77777777777777777777777777777776");
        //Metadata fake4 = new Metadata("localhost", "8003", "77777777777777777777777777777777", "99999999999999999999999999999998");

        // can uncomment this to see if KVServer receives metadata from ECS
        //ecs.initKVServer(fake1, 0, "LRU", true);
        //ecs.initKVServer(fake2, 0, "LRU", true);
        //ecs.initKVServer(fake1, 0, "LRU", true);
        //ecs.initKVServer(fake2, 0, "LRU", true);

        ecs.start("localhost", 8000);
        ecs.start("localhost", 8001);
        //ecs.start("localhost", 8002);
        //ecs.start("localhost", 8003);

        //ecs.stop("localhost", 8000);
        //ecs.stop("localhost", 8001);
        //ecs.stop("localhost", 8002);
        //ecs.stop("localhost", 8003);

        //ecs.shutDown("localhost", 8000);
        //ecs.shutDown("localhost", 8001);
        //ecs.shutDown("localhost", 8002);
        //ecs.shutDown("localhost", 8003);

        //ecs.lockWrite("localhost", 8000);
        //ecs.lockWrite("localhost", 8001);
        //ecs.lockWrite("localhost", 8002);
        //ecs.lockWrite("localhost", 8003);

        ecs.unlockWrite("localhost", 8000);
        ecs.unlockWrite("localhost", 8001);
        //ecs.unlockWrite("localhost", 8002);
        //ecs.unlockWrite("localhost", 8003);


        // TODO: incomplete on KVServer side
        //ecs.moveData("localhost", 8000,                "00000000000000000000000000000000",                "ffffffffffffffffffffffffffffffff");

        ecs.removeServer("127.0.0.1",9002);

        //update & send each server's metadata to all servers
        //ecs.sendUpdatedMetadata();

        ecs.initService(2,10,"LRU",true);

    }
}

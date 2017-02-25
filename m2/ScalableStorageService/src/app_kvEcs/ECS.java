package app_kvEcs;

import app_kvServer.Metadata;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by rahmanz5 on 2/24/2017.
 */
public class ECS implements ECSInterface {

    private SortedMap<String, Metadata> hashRing;
    private Socket ecsSocket;

    public ECS(){
        hashRing = new TreeMap<String, Metadata>();
    }

    @Override
    public void initKVServer(Metadata m, int cacheSize, String strategy, boolean  log) {
        // hash the server
        String serverHash = "";
        try {
            serverHash = md5.HashS(m.host + ":" + m.port);
        } catch (Exception ex){

        }

        // put it into the hash ring
        hashRing.put(serverHash, m);

        /*
        // update the ranges of the servers in clockwise order
        updateRanges();

        sshSession mySsh = new sshSession();
        String user = "rahmanz5", host = "ug222.eecg.toronto.edu";//128.100.13.<>
        int port = 22;

        mySsh.connectSsh(user,host,port);

        String cmd = "java -jar ms2-server.jar "+ m.host + " " + m.port +" "+cacheSize +" " + strategy  + " " + log;
        String directory = "cd ece419/m2/ScalableStorageService";

        mySsh.runServer(directory, cmd);

        mySsh.session.disconnect();//can be moved to destructor

        */
        // start a TCP connection and update each server's metadata

        updateServerMetadata();
    }

    private void updateServerMetadata()
    {
        //Protocol: ECS-METADATA-<HostIP>,<Port>,<StartRange>,<EndRange>,0
        for(Map.Entry<String,Metadata> entry: hashRing.entrySet())
        {
            String msgType = "ECS-METADATA-";

            String key = entry.getKey();
            Metadata m = entry.getValue();

            // build a byte[] of metadata, and send it via TCP to the server
            int mDatalen = msgType.length() + entry.getValue().host.length() + entry.getValue().port.length()
                        + entry.getValue().startHash.length() +
                    entry.getValue().endHash.length() + 4 + 1;//4 for ",", 1 for 0 -> byte-terminator

            byte[] mData = new byte[mDatalen];

            byte[] hostB = (entry.getValue().host + ",").getBytes();
            byte[] portB = (entry.getValue().port + ",").getBytes();
            byte[] srtRangeB = (entry.getValue().startHash + ",").getBytes();
            byte[] endRangeB = (entry.getValue().endHash + ",").getBytes();

            System.arraycopy(msgType.getBytes(),0,mData,0,msgType.length());
            System.arraycopy(hostB,0,mData,msgType.length(),hostB.length);
            System.arraycopy(portB,0,mData,msgType.length() + hostB.length,portB.length);
            System.arraycopy(srtRangeB,0,mData,msgType.length() + hostB.length
                            + portB.length,srtRangeB.length);
            System.arraycopy(endRangeB,0,mData,msgType.length() + hostB.length + portB.length
                            + srtRangeB.length,endRangeB.length);

            //System.out.println("MetaData byte array: " + new String(mData));//for debugging

            sendViaTCP(entry.getValue().host,Integer.parseInt(entry.getValue().port),mData);
        }

    }

    private void updateRanges() {
        String prev = null;
        for(Map.Entry<String,Metadata> entry: hashRing.entrySet()){
            String key = entry.getKey();
            Metadata m = entry.getValue();

            if(prev != null) {
                m.endHash = key;
            } else {
                //System.out.println(new String(new char[32]).replace("\0", "f"));
                m.endHash = new String(new char[32]).replace("\0", "f");//"ffffffffffffffffffffffffffffffff";
            }

            //TODO: need to properly deal with boundaries (this is a quick fix)
            if(prev != null){
                m.startHash = prev;
            } else {
                m.startHash = hashRing.lastKey();
            }

            hashRing.put(key, m);
            prev = key;
        }
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

        byte[] byteMsg = new byte[msgType.length() + (1+startRange.length()) + (1+endRange.length()) + 2];//last +2
        // for 0 terminator

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

            writeToSock.write(data,0,data.length);
            writeToSock.flush();

            this.ecsSocket.close();

        }
        catch (Exception ex)
        {
            System.out.println("ECS failed to send data to KVServer: " + host + ":" + port);
            ex.printStackTrace();
        }
    }

    public static void main(String[] args){
        ECS ecs = new ECS();
        //Metadata fake1 = new Metadata("128.100.13.222", "8000", "", "");
        //Metadata fake2 = new Metadata("128.100.13.222", "8001", "", "");
        //Metadata fake3 = new Metadata("128.100.13.222", "8002", "", "");
        Metadata fake4 = new Metadata("localhost", "8000", "10ab", "a24f");

        /*ecs.start("localhost", 8000);
        ecs.stop("localhost", 8000);
        ecs.shutDown("localhost", 8000);
        ecs.lockWrite("localhost", 8000);
        ecs.unlockWrite("localhost", 8000);
        ecs.moveData("localhost", 8000,
                "00000000000000000000000000000000",
                "ffffffffffffffffffffffffffffffff");*/
        //ecs.initKVServer(fake4,100,"LRU", true);
    }
}

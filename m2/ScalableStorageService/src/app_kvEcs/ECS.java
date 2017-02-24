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

        // start a TCP connection and update each server's metadata

        updateServerMetadata();
    }

    private void updateServerMetadata()
    {
        for(Map.Entry<String,Metadata> entry: hashRing.entrySet())
        {
            String key = entry.getKey();
            Metadata m = entry.getValue();

            // build a byte[] of metadata, and send it via TCP to the server
            int mDatalen = entry.getValue().host.length() + entry.getValue().port.length()
                        + entry.getValue().startHash.length() +
                    entry.getValue().endHash.length() + 3;//3 for ","

            byte[] mData = new byte[mDatalen];

            byte[] hostB = (entry.getValue().host + ",").getBytes();
            byte[] portB = (entry.getValue().port + ",").getBytes();
            byte[] srtRangeB = (entry.getValue().startHash + ",").getBytes();
            byte[] endRangeB = (entry.getValue().endHash).getBytes();

            System.arraycopy(hostB,0,mData,0,hostB.length);
            System.arraycopy(portB,0,mData,hostB.length,portB.length);
            System.arraycopy(srtRangeB,0,mData,hostB.length + portB.length,srtRangeB.length);
            System.arraycopy(endRangeB,0,mData,hostB.length + portB.length + srtRangeB.length,endRangeB.length);

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
        // send a TCP message that says, accept clients
        byte[] byteMsg = {'E','C','S','-','S','T','A','R','T',0};
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void stop(String host, int port)
    {
        byte[] byteMsg = {'E','C','S','-','S','T','O','P',0};//E for End
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void shutDown(String host, int port)
    {
        byte[] byteMsg = {'E','C','S','-','S','H','U','T','D','O','W','N',0};//T for terminate
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void lockWrite(String host, int port)
    {
        byte[] byteMsg = {'E','C','S','-','L','O','C','K','W','R','I','T','E',0};//R for read-only
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void unlockWrite(String host, int port)
    {
        byte[] byteMsg = {'E','C','S','-','U','N','L','O','C','K','W','R','I','T','E',0};//W for read-write
        sendViaTCP(host, port, byteMsg);
    }

    @Override
    public void moveData(String host, int port, String startRange, String endRange)
    {
        //Protocol: ECS0M0<start-range>0<end-range>
        byte[] byteMsg = new byte[5 + (1+startRange.length()) + (2+endRange.length())];//5 for ECS0M

        byteMsg[0] = 'E';
        byteMsg[1] = 'C';
        byteMsg[2] = 'S';
        byteMsg[3] = '-';
        byteMsg[4] = 'M';

        byteMsg[5] = '-';
        int startRIdx = 6;
        byte[] startBytes = startRange.getBytes();
        System.arraycopy(startBytes,0,byteMsg,startRIdx,startBytes.length);
        byteMsg[startRIdx+startBytes.length] = '-';

        int endRIdx = (startRIdx+startBytes.length) + 1;
        byte[] endBytes = endRange.getBytes();
        System.arraycopy(endBytes,0,byteMsg,endRIdx,endBytes.length);
        byteMsg[byteMsg.length-1] = 0;

        //System.out.println("start: " + startRange + "\n" + "end: " + endRange + "\n" + "Byte Msg: " + new String(byteMsg));//for debugging

        sendViaTCP(host, port, byteMsg);
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
        Metadata fake4 = new Metadata("localhost", "8000", "", "");

        ecs.lockWrite("localhost", 8000);
        ecs.unlockWrite("localhost", 8000);
        ecs.start("localhost", 8000);
        ecs.stop("localhost", 8000);
        ecs.shutDown("localhost", 8000);
        ecs.moveData("localhost", 8000,
                "00000000000000000000000000000000",
                "00000000000000000000000000000000");

    }
}

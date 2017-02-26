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
        byte[] mData;
        String msg = "ECS-METADATA-";
        int index = 1;

        for(Map.Entry<String,Metadata> entry: hashRing.entrySet())
        {
            String key = entry.getKey();
            Metadata m = entry.getValue();

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

        byte[] data = msg.getBytes();
        mData = new byte[data.length+1];
        int i = 0;
        for(i = 0; i <data.length; i++){
            mData[i] = data[i];
        }
        mData[i] = 0;
        for(Map.Entry<String, Metadata> entry: hashRing.entrySet()){
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

        try {
            hashRing.remove(md5.HashS(host));
            updateServerMetadata();
        } catch (Exception ex){
            System.out.println(ex);
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

    public static void main(String[] args){
        ECS ecs = new ECS();

        // use this for testing on ug machines
        //Metadata fake1 = new Metadata("128.100.13.222", "8000", "asfdsaf", "fdasfddas");


        // use this for testing wrap around (make sure that a metadata file exists in the folder location where the kvps are stored)
        Metadata fake1 = new Metadata("localhost", "8000", "f0000000000000000000000000000000", "c0000000000000000000000000000000");
        Metadata fake2 = new Metadata("localhost", "8001", "c0000000000000000000000000000001", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        // use this for testing on localhost
        //Metadata fake1 = new Metadata("localhost", "8000", "99999999999999999999999999999999", "33333333333333333333333333333332");
        //Metadata fake2 = new Metadata("localhost", "8001", "33333333333333333333333333333333", "55555555555555555555555555555554");
        //Metadata fake3 = new Metadata("localhost", "8002", "55555555555555555555555555555555", "77777777777777777777777777777776");
        //Metadata fake4 = new Metadata("localhost", "8003", "77777777777777777777777777777777", "99999999999999999999999999999998");

        // can uncomment this to see if KVServer recives metadata from ECS
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

    }
}

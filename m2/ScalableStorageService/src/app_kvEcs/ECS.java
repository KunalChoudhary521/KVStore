package app_kvEcs;

import app_kvServer.Metadata;
import com.sun.javafx.collections.transformation.SortedList;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by Haashir on 2/24/2017.
 */
public class ECS implements ECSInterface {

    private SortedMap<String, Metadata> hashRing;

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

        String cmd = "java -jar ms2-server.jar " + m.port +" "+cacheSize +" " + strategy  + " " + log;
        String directory = "cd ece419/m2/ScalableStorageService";

        mySsh.runServer(directory, cmd);

        mySsh.session.disconnect();//can be moved to destructor

        // start a TCP connection and update each server's metadata

        updateServerMetadata();
    }

    private void updateServerMetadata() {
        for(Map.Entry<String,Metadata> entry: hashRing.entrySet()){
            String key = entry.getKey();
            Metadata m = entry.getValue();
            update(m, m.host, Integer.parseInt(m.port));
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
                m.endHash = "ffffffffffffffffffffffffffffffff";
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
    public void start(String host, int port) {

    }

    @Override
    public void stop(String host, int port) {

    }

    @Override
    public void shutDown(String host, int port) {

    }

    @Override
    public void lockWrite(String host, int port) {

    }

    @Override
    public void unlockWrite(String host, int port) {

    }

    @Override
    public void moveData(String startRange, String endRange, String host, int port) {

    }

    @Override
    public void update(Metadata newMetadata, String host, int port) {

    }

    public static void main(String[] args){
        ECS ecs = new ECS();
        Metadata fake1 = new Metadata("localhost", "8000", "", "");
        Metadata fake2 = new Metadata("localhost", "8001", "", "");
        Metadata fake3 = new Metadata("localhost", "8002", "", "");
        ecs.initKVServer(fake1, 0, "LRU", true);
        ecs.initKVServer(fake2, 0, "LRU", true);
        ecs.initKVServer(fake3, 0, "LRU", true);
    }
}

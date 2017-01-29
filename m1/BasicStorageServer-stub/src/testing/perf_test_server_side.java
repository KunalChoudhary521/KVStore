package testing;
import app_kvServer.KVServer;
import org.junit.Rule;
import org.junit.Test;
import java.util.Random;

import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import org.junit.rules.TestRule;
import java.lang.Object;

/**
 * Created by yy on 2017-01-29.
 */
public class perf_test_server_side extends Thread{
    private KVServer serv;
/*    private KVServer server_LRU;
    private KVServer server_LFU;
    private KVServer server_FIFO;*/

    public perf_test_server_side(int size, int port, String strat, String file) {

        /*this.server_LFU = new KVServer(8082,size,"LFU","\\TestFile1.txt",false);
        this.server_LRU = new KVServer(8080, size,"LRU","\\TestFile2.txt",false);
        this.server_FIFO =  new KVServer(8091, size,"FIFO","\\TestFile3.txt",false);
        */
        this.serv = new KVServer(port, size, strat, file, false);

    }

    public void tearDown() {
        /*kvClient_LRU.disconnect();
        kvClient_LFU.disconnect();
        kvClient_FIFO.disconnect();
        server_FIFO.Stop();
        server_LFU.Stop();
        server_LRU.Stop();*/
        this.serv.Stop();
    }
    public void run(){
        this.serv.Run();
    }
/*
    public void test_performance_LRU_80_20(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2, "key_" + i + "_" + 3};
                String[] values = {"value1", "value2", "value3", "value4"};
                for (int j = 0; j < 4; j++) {
                    kvClient_LRU.put(keys[j], values[j]);
                }
                for (int j = 0; j < 2; j++) {
                    kvClient_LRU.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(4));
                }
            }
        }
        catch(Exception e){
            System.out.println("lru_80_20");
            System.out.println(e.getMessage());
        }
    }

    public void test_performance_LRU_50_50(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2};
                String[] values = {"value1", "value2", "value3"};
                for (int j = 0; j < 3; j++) {
                    kvClient_LRU.put(keys[j], values[j]);
                }
                for (int j = 0; j < 3; j++) {
                    kvClient_LRU.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(3));
                }
            }
        }
        catch(Exception e){
            System.out.println("lru_50_50");
            System.out.println(e.getMessage());
        }
    }
    public void test_performance_LRU_20_80(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2, "key_" + i + "_" + 3};
                String[] values = {"value1", "value2", "value3", "value4"};
                for (int j = 0; j < 2; j++) {
                    kvClient_LRU.put(keys[j], values[j]);
                }
                for (int j = 0; j < 4; j++) {
                    kvClient_LRU.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(2));
                }
            }
        }
        catch(Exception e){
            System.out.println("lru_20_80");
            System.out.println(e.getMessage());
        }
    }

    public void test_performance_LFU_80_20(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2, "key_" + i + "_" + 3};
                String[] values = {"value1", "value2", "value3", "value4"};
                for (int j = 0; j < 4; j++) {
                    kvClient_LFU.put(keys[j], values[j]);
                }
                for (int j = 0; j < 2; j++) {
                    kvClient_LFU.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(4));
                }
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void test_performance_LFU_50_50(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2};
                String[] values = {"value1", "value2", "value3"};
                for (int j = 0; j < 3; j++) {
                    kvClient_LFU.put(keys[j], values[j]);
                }
                for (int j = 0; j < 3; j++) {
                    kvClient_LFU.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(3));
                }
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    public void test_performance_LFU_20_80(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2, "key_" + i + "_" + 3};
                String[] values = {"value1", "value2", "value3", "value4"};
                for (int j = 0; j < 2; j++) {
                    kvClient_LFU.put(keys[j], values[j]);
                }
                for (int j = 0; j < 4; j++) {
                    kvClient_LFU.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(2));
                }
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void test_performance_FIFO_80_20(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2, "key_" + i + "_" + 3};
                String[] values = {"value1", "value2", "value3", "value4"};
                for (int j = 0; j < 4; j++) {
                    kvClient_FIFO.put(keys[j], values[j]);
                }
                for (int j = 0; j < 2; j++) {
                    kvClient_FIFO.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(4));
                }
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void test_performance_FIFO_50_50(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2};
                String[] values = {"value1", "value2", "value3"};
                for (int j = 0; j < 3; j++) {
                    kvClient_FIFO.put(keys[j], values[j]);
                }
                for (int j = 0; j < 3; j++) {
                    kvClient_FIFO.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(3));
                }
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    public void test_performance_FIFO_20_80(){
        Random rg = new Random(3);
        try {
            for (int i = 0; i < 100; i++) {
                String[] keys = {"key_" + i + "_" + 0, "key_" + i + "_" + 1, "key_" + i + "_" + 2, "key_" + i + "_" + 3};
                String[] values = {"value1", "value2", "value3", "value4"};
                for (int j = 0; j < 2; j++) {
                    kvClient_FIFO.put(keys[j], values[j]);
                }
                for (int j = 0; j < 4; j++) {
                    kvClient_FIFO.get("key_" + rg.nextInt(i) + "_" + rg.nextInt(2));
                }
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }*/


}

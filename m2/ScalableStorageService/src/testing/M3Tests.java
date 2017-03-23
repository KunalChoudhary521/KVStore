package testing;

import app_kvEcs.ECS;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by yy on 2017-03-23.
 */
public class M3Tests extends TestCase {
    private KVStore kvClient;
    private ECS ecs;

    /*
    Ensure can't initialize less than 3 servers
     */
    @Test
    public void test_initialize_few_servers() {
        Exception ex = null;
        try {
            ecs = new ECS(true);
            ecs.initKVServer(2, 10, "LRU", false);
        } catch (Exception e) {
            ex = e;
        }
        ecs.shutDown();
        assertTrue(ex != null);
    }

    /*
        Ensure can initialize at least3 servers
         */
    @Test
    public void test_more_than_least3_servers() {
        Exception ex = null;
        try {
            ecs = new ECS(true);
            ecs.initKVServer(4, 10, "LRU", false);
        } catch (Exception e) {
            ex = e;
        }
        ecs.shutDown();
        assertTrue(ex == null);
    }

    /*
    Ensure can't remove servers such that have less than 3 servers
     */
    @Test
    public void test_remove_server_when_not_allowed_to() {
        Exception ex = null;
        try {
            ecs = new ECS(true);
            ecs.initKVServer(3, 10, "LRU", false);
            String[] s1 = ecs.getRunningServers().get(0).split(":");
            ecs.removeNode(s1[0], Integer.parseInt(s1[1]));
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex != null);
    }

    /*
    Ensure getting from multiple servers
     */
    @Test
    public void test_get_reps() {
        String key = "k1";
        String value = "v1";
        try {
            ecs = new ECS(true);
            ecs.initKVServer(3, 10, "LRU", false);
            String[] s1 = ecs.getRunningServers().get(0).split(":");
            String[] s2 = ecs.getRunningServers().get(1).split(":");
            String[] s3 = ecs.getRunningServers().get(2).split(":");
            ecs.start();
            ecs.unlockWrite(s1[0], Integer.parseInt(s1[1]));
            ecs.unlockWrite(s2[0], Integer.parseInt(s2[1]));
            ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));
            kvClient = new KVStore(s1[0], Integer.parseInt(s1[1]));
            kvClient.connect();
            kvClient.put(key, value);
        } catch (Exception e) {

        }
        Set<String> servers_contacted = new LinkedHashSet<>();
        KVMessage res = null;
        for (int i = 0; i < 100; i++) {
            try {
                res = null;
                res = kvClient.get(key);
                servers_contacted.add(res.getSource());
            } catch (Exception e) {
                if (res != null) {
                    servers_contacted.add(res.getSource());
                }
            }
        }
        assertTrue(servers_contacted.size() > 1);
    }

    /*
    Ensure only puts to 1 server
     */
    @Test
    public void test_put_no_reps() {
        String key = "k1";
        String value = "v1";
        try {
            ecs = new ECS(true);
            ecs.initKVServer(3, 10, "LRU", false);
            String[] s1 = ecs.getRunningServers().get(0).split(":");
            String[] s2 = ecs.getRunningServers().get(1).split(":");
            String[] s3 = ecs.getRunningServers().get(2).split(":");
            ecs.start();
            ecs.unlockWrite(s1[0], Integer.parseInt(s1[1]));
            ecs.unlockWrite(s2[0], Integer.parseInt(s2[1]));
            ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));
            kvClient = new KVStore(s1[0], Integer.parseInt(s1[1]));
            kvClient.connect();

        } catch (Exception e) {

        }
        Set<String> servers_contacted = new LinkedHashSet<>();
        KVMessage res = null;
        for (int i = 0; i < 100; i++) {
            try {
                res = null;
                res = kvClient.put(key, value);
                servers_contacted.add(res.getSource());
            } catch (Exception e) {
                if (res != null) {
                    servers_contacted.add(res.getSource());
                }
            }
        }
        assertTrue(servers_contacted.size() == 1);
    }

    /*
    Ensure failure is detected
     */
    @Test
    public void test_5() {
        //to implement
        assertTrue(true);
    }

    /*
    Ensure deleted item is deleted on all replicas
     */
    @Test
    public void test_6() {
        //to implement
        assertTrue(true);
    }

    /*
    Ensure updated item is updated on all replicas
     */
    @Test
    public void test_7() {
        //to implement
        assertTrue(true);
    }

    /*
    To Determine
     */
    @Test
    public void test_9() {
        assertTrue(true);
    }


    /*
    To Determine
     */
    @Test
    public void test_10() {
        assertTrue(true);
    }
}

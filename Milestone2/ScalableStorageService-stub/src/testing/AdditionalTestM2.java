package testing;

import client.KVStore;
import common.Metadata;
import common.md5;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import ecs.ECS;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

import java.math.BigInteger;
import java.util.Map;

public class AdditionalTestM2 extends TestCase {

    //add your test cases, at least 3
    private ECS testEcs;

    //Called before each testcase
    @Before
    public void setUp() {
        try {
            new LogSetup("logs/testing/additionalTestsM2.log", Level.ALL);
            testEcs = new ECS("ecs.config");
        } catch (Exception e) {

        }
    }

    //Called after each testcase
    @After
    public void tearDown() {
        testEcs.shutDown();//shutdown KVServer so that it is not left running
        testEcs = null;
    }

    /**
     * Add 3 servers to the ring and check if they are indeed in the ring
     */
    @Test
    public void testAddServersToRing() {
        String[] address = {"127.0.0.1", "192.168.0.10", "128.0.10.11" };
        int[] port = {9000, 30_000, 50_000};

        Exception ex = null;
        BigInteger serverHash = null;
        Metadata serverMetaData;

        for(int i = 0; i < address.length; i++) {
            try {
                testEcs.addToRing(address[i], port[i]);
                serverHash = md5.HashInBI(address[i] + ":" + port[i]);
            }
            catch (Exception e) {
                ex = e;
            }
        }

        for(int i = 0; i < address.length; i++) {
            try {
                serverHash = md5.HashInBI(address[i] + ":" + port[i]);
            }
            catch (Exception e) {
                ex = e;
            }
            serverMetaData = testEcs.getHashRing().get(serverHash);
            assertTrue(ex == null && serverMetaData.address.equals(address[i])
                        && serverMetaData.port == port[i]);
        }
    }

    public void testAddOneNode()
    {
        String k ="key1", v = "value1";
        Exception ex = null;
        KVAdminMessage resToECS = null;
        KVMessage resToClient = null;
        KVStore kvClient;
        Metadata firstServer;
        try {
            resToECS = testEcs.addNode("ALL",10,"LRU", true);
        }
        catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && resToECS.getStatus() == KVAdminMessage.StatusType.ADD_SUCCESS);

        //connect a KVClient. Put (k,v) and try to retrieve it
        try {
            firstServer = testEcs.getHashRing().get(testEcs.getHashRing().firstKey());

            kvClient = new KVStore(firstServer.address, firstServer.port);
            kvClient.connect();

            resToClient = kvClient.put(k, v);
            resToClient = kvClient.get(k);

            kvClient.disconnect();
        }
        catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && resToClient.getValue().equals(v));
    }

    public void testInitWith3()
    {
        int numOfNodes = 3;//init 3 KVServers
        Exception ex = null;
        KVAdminMessage resToECS = null;

        try {
            resToECS = testEcs.initService(numOfNodes,"ALL",10,"LRU");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && resToECS.getStatus() == KVAdminMessage.StatusType.INIT_ALL);

        int connectionsMade = 0;
        KVStore kvClient;
        Metadata serverInfo;
        //Try connect KVClient to each KVServer
        try {
            for (Map.Entry<BigInteger, Metadata> entry : testEcs.getHashRing().entrySet())
            {
                serverInfo = entry.getValue();
                kvClient = new KVStore(serverInfo.address, serverInfo.port);
                //If one of the server crashes, then connection is NOT established and exception is thrown
                kvClient.connect();
                kvClient.disconnect();
                connectionsMade++;
            }
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && connectionsMade == numOfNodes);
    }

    public void testRemoveNode()
    {

    }

    //try to put from KVClient at a KVServer while write is locked
    public void testPutWhileWriteLock()
    {

    }

    public void testPutAtWrongServer()
    {

    }

    public void testGetFromWrongServer()
    {

    }

    public void testPutWhileSeverStop()
    {

    }

    public void testGetWhileSeverStop()
    {

    }

    public void testShutDownOneServer()
    {
        Exception ex = null;
        try {
            //make sure the server is running first
            testEcs.shutDownNode("127.0.0.1", 9000);
        }
        catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }

    public void testShutDownAllServers()
    {

    }
}

package testing;

import app_kvServer.KVServer;
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
        Exception ex1 = null, ex2 = null;
        KVAdminMessage resToECS = null;
        KVMessage resToClient = null;
        KVStore kvClient;
        Metadata firstServer;
        try {
            resToECS = testEcs.addNode("ALL",10,"LRU", true);
        }
        catch (Exception e) {
            ex1 = e;
        }

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
            ex2 = e;
        }

        assertTrue(ex1 == null && resToECS.getStatus() == KVAdminMessage.StatusType.ADD_SUCCESS
                         && ex2 == null && resToClient.getValue().equals(v));
    }

    public void testInitWith3()
    {
        int numOfNodes = 3;//init 3 KVServers
        Exception ex = null;
        KVAdminMessage resToECS = null;

        resToECS = testEcs.initService(numOfNodes,"ALL",10,"LRU");

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

        assertTrue(ex == null && resToECS.getStatus() == KVAdminMessage.StatusType.INIT_ALL
                    && connectionsMade == numOfNodes);
    }

    public void testRemoveNode()
    {
        //Add 2 nodes, remove 1st node, check range of 2nd node (startHash = endHash + 1)
        int numOfNodes = 2;//add 2 KVServers
        Exception ex = null;
        KVAdminMessage resToECS1, resToECS2;

        //Add 2 KVServers
        resToECS1 = testEcs.initService(numOfNodes,"ALL",10,"LRU");

        Metadata[] servers = testEcs.getHashRing().values()
                                    .toArray(new Metadata[testEcs.getHashRing().size()]);
        Metadata firstS = servers[0];

        //remove one of them
        resToECS2 = testEcs.removeNode(firstS.address, firstS.port);


        servers = testEcs.getHashRing().values().toArray(new Metadata[testEcs.getHashRing().size()]);
        Metadata secondS = servers[0];

        //check range of 2nd node (startHash = endHash + 1)
        BigInteger sHash = new BigInteger(secondS.startHash,16);
        BigInteger eHash = new BigInteger(secondS.endHash,16);

        assertTrue(resToECS1.getStatus() == KVAdminMessage.StatusType.INIT_ALL
                && resToECS2.getStatus() == KVAdminMessage.StatusType.REMOVE_SUCCESS
                && sHash.equals(eHash.add((BigInteger.ONE))));
    }

    //try to put from KVClient at a KVServer while write is locked
    public void testPutWhileWriteLock()
    {
        boolean result = false;
        String k ="k1", v = "v1";
        Exception ex1 = null, ex2 = null;
        KVAdminMessage resToECS = null;
        KVMessage resToClient = null;
        KVStore kvClient;
        Metadata firstServer;
        try {
            resToECS = testEcs.addNode("ALL",10,"LRU", true);
            result = testEcs.setWriteStatus("127.0.0.1", 9000, "LOCKWRITE");
        }
        catch (Exception e) {
            ex1 = e;
        }

        //connect a KVClient. Try to put (k,v)
        try {
            firstServer = testEcs.getHashRing().get(testEcs.getHashRing().firstKey());

            kvClient = new KVStore(firstServer.address, firstServer.port);
            kvClient.connect();

            resToClient = kvClient.put(k, v);

            kvClient.disconnect();
        }
        catch (Exception e) {
            ex2 = e;
        }

        assertTrue(ex1 == null && ex2 == null && result == true
                && resToClient.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK
                && resToECS.getStatus() == KVAdminMessage.StatusType.ADD_SUCCESS);
    }

    /**   1 Add 1st server
          2 Add a few KVpairs (ex. 10)
          3 Add 2nd server
          4 Check if the keys transferred fall in range of respective KVServers
     */
    public void testTransferKVPair1()
    {
        Exception ex1 = null, ex2 = null, ex3 = null;
        KVAdminMessage resToECS = null;
        KVMessage resToClient = null;
        KVStore kvClient;
        Metadata firstServer;
        int kvPairs = 10;

        try {
            //a lot of puts & gets cause KVServer to hang if logLevel is NOT OFF
            resToECS = testEcs.addNode("OFF",10,"LRU", true);
        } catch (Exception e) {
            ex1 = e;
        }

        try {
            firstServer = testEcs.getHashRing().get(testEcs.getHashRing().firstKey());

            kvClient = new KVStore(firstServer.address, firstServer.port);
            kvClient.connect();

            for(int i = 1; i < kvPairs; i++) {
                resToClient = kvClient.put("k" + i, "v" + i);//(k1,v1); (k2,v2); ...
            }

            kvClient.disconnect();
        } catch (Exception e) {
            ex2 = e;
        }

        try {
            resToECS = testEcs.addNode("INFO",10,"LRU", true);
        } catch (Exception e) {
            ex3 = e;
        }

        assertTrue(ex1 == null && ex2 == null && ex3 == null &&
                    resToClient.getStatus() == KVMessage.StatusType.PUT_SUCCESS &&
                    resToECS.getStatus() == KVAdminMessage.StatusType.ADD_SUCCESS);
    }

    /**  1 Add 2 servers
         2 Add a few KVpairs (ex. 10) (fix SERVER_NOT_RESPONSIBLE)
         3 Remove 1 server
         4 Check if the keys transferred fall in range of respective KVServers
     */
    public void testTransferKVPair2()
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

package testing;

import ecs.ECS;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;

import junit.framework.TestCase;

public class AdditionalTestM2 extends TestCase {

    //add your test cases, at least 3
    private ECS testEcs;

    public void setUp() {
        try {
            new LogSetup("logs/testing/additionalTestsM2.log", Level.ALL);
            testEcs = new ECS("ecs.config");
        } catch (Exception e) {

        }
    }

    public void tearDown() {
        testEcs = null;
    }

    @Test
    public void testAddServersToRing() {
        String address = "127.0.0.1";
        int port = 9000;

        Exception ex = null;

        try {
            testEcs.addToRing(address, port);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(testEcs != null);
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
}

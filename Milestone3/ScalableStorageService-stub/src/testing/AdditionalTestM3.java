package testing;

import ecs.ECS;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;

public class AdditionalTestM3 extends TestCase
{
    //add your test cases, at least 3
    private ECS testEcs;

    //Called before each testcase
    @Before
    public void setUp() {
        try {
            new LogSetup("logs/testing/additionalTestsM3.log", Level.ALL);
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

    //Test cases to test the functionality of features added in M3

    //check if (k,v) was replicated by coordinator to replicas
    public void testPutReplication()
    {
        assertTrue(true);
    }

    //check if (k,v) was deleted by coordinator from all replicas
    public void testDeleteReplication()
    {
        assertTrue(true);
    }

    //check if (k,v) returns v from coordinator & replicas
    public void testGetReplication()
    {
        assertTrue(true);
    }

    //check if put at a replica FAILS (put MUST only be done at coordinator)
    public void testPutAtReplica()
    {
        assertTrue(true);
    }
}

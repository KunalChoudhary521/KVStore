package testing;

import java.io.IOException;

import common.messages.KVAdminMessage;
import ecs.ECS;
import org.apache.log4j.Level;

import logger.LogSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ ConnectionTest.class, InteractionTest.class,AdditionalTest.class })
public class AllTests {

    private static ECS testEcs;

    //Called ONCE before all test classes in @Suite.SuiteClasses() have run
    @BeforeClass
    public static void setUp()
    {
        try {
            new LogSetup("logs/testing/AllTests.log", Level.ALL);
            //new KVServer(9000, "ALL", 5, "LRU").start();//9000 ALL 10 LRU
            testEcs = new ECS("ecs.config");
            KVAdminMessage resToECS = testEcs.initService(1,"ALL",20,"LRU");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //Called ONCE after all test classes in @Suite.SuiteClasses() have run
    @AfterClass
    public static void tearDown()
    {
        testEcs.shutDown();
    }
}

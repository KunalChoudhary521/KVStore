package testing;

import app_kvEcs.ECS;
import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			ECS ecs = new ECS(true);
			ecs.initService(2,10,"LRU");
			ecs.start();
			ecs.unlockWrite("127.0.0.1",8080);
			ecs.unlockWrite("127.0.0.1",9000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
	//	clientSuite.addTestSuite(ConnectionTest.class);
	//	clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class); 
		return clientSuite;
	}
	
}

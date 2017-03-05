package testing;

import java.net.UnknownHostException;

import app_kvEcs.ECS;
import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	private ECS ecs;
	public void setUp() {
		ecs = new ECS(true);
		ecs.initService(2,10,"LRU");
		ecs.start();
		ecs.unlockWrite("127.0.0.1",8080);
		ecs.unlockWrite("127.0.0.1",9000);
	}
	public void tearDown() {
		ecs.shutDown();
	}
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 8080);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 8080);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}


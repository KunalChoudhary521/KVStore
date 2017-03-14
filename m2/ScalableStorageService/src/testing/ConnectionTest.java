package testing;

import java.net.UnknownHostException;

import app_kvEcs.ECS;
import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	private ECS ecs;
	public void testConnectionSuccess() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");		ecs.start();
		Exception ex = null;
		
		KVStore kvClient = new KVStore(s1[0], Integer.parseInt(s1[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		ecs.shutDown();
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");		ecs.start();
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 8080);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		ecs.shutDown();
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");		ecs.start();
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		ecs.shutDown();
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}


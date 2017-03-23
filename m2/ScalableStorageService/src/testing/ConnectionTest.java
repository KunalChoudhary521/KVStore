package testing;

import app_kvEcs.ECS;
import client.KVStore;
import junit.framework.TestCase;

import java.net.UnknownHostException;


public class ConnectionTest extends TestCase {
	private ECS ecs;
	public void testConnectionSuccess() {
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();

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
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
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
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
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


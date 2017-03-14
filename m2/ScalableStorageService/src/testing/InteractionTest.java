package testing;

import app_kvEcs.ECS;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class InteractionTest extends TestCase {
	private ECS ecs;
	private KVStore kvClient;
	
	/*public void setUp() {
		ecs = new ECS(false);
		ecs.initService(2,10,"LRU");
		ecs.start();
		ecs.unlockWrite("127.0.0.1",8080);
		ecs.unlockWrite("127.0.0.1",9000);
		kvClient = new KVStore("localhost", 8080);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		try{
			ecs.removeNode("127.0.0.1",8080);
			ecs.removeNode("127.0.0.1",9000);
			ecs.shutDown();
		}catch(Exception ex){
			//donothing;
		}
	}*/
	
	
	@Test
	public void testPut() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));

		String key = "putfoo";
		String value = "putbar";
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.connect();
			response = kvClient.put(key, value);
			
		} catch (Exception e) {
			ex = e;
		}
		try{
			kvClient.put(key,"null");
			kvClient.disconnect();
			ecs.shutDown();
		}catch(Exception ex2){
		//donothing
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));

		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		ecs.shutDown();
		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));
		
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.connect();
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}
		try{
			kvClient.put(key,"null");
			kvClient.disconnect();
			ecs.shutDown();
		}catch(Exception ex2){
			//donothing
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));

		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.connect();
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			kvClient.disconnect();
			ecs.shutDown();
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "getfoo";
		String value = "bar";
		KVMessage response = null;

		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));

		Exception ex = null;

			try {
				kvClient.connect();
				kvClient.put(key, value);
				response = kvClient.get(key);
				kvClient.disconnect();
				ecs.shutDown();
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		ecs = new ECS(true);
		ecs.initKVServer(1,10,"LRU",false);
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));


		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.connect();
			response = kvClient.get(key);
			kvClient.disconnect();
			ecs.shutDown();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	


}

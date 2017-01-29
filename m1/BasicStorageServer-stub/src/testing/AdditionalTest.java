package testing;

import org.junit.Test;

import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import stress.GetClient;

public class AdditionalTest extends TestCase {

	// TODO add your test cases, at least 3
	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 8080);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}


	@Test
	public void test_get_long_key() {
		String key = "The Quick Brown Fox Jumped Over The Lazy Dog";
		KVMessage response = null;
		Exception ex = null;

		try{
			kvClient.get(key);
		} catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() ==StatusType.GET_ERROR);
	}
	@Test
	public void test_put_long_payload() {
		String key = "foo";
		String value = "bar";
		for (int i = 0; i<=120*1024;i++){
			value += "bar";
		}
		KVMessage response = null;
		Exception ex = null;
		try{
			kvClient.put(key,value);
			response = kvClient.get(key);
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}
	@Test
	public void test_put_long_key() {
		String key = "The Quick Brown Fox Jumped Over The Lazy Dog";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try{
			kvClient.put(key,value);
		} catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() ==StatusType.PUT_ERROR);
	}
	@Test
	public void test_delete_get() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			response = kvClient.get(key);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	@Test
	public void test_update_shorter() {
		String key = "foo";
		String value = "bar";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "r");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
	}
	@Test
	public void test_update_longer() {
		String key = "foo";
		String value = "to";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "The Quick Brown Fox Jumped Over The Lazy Dog");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
	}
	@Test
	public void test_get_disconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.get(key);

		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}
	@Test
	public void test_get_with_0() {
		String key = "0foo0";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}
	@Test
	public void test_put_with_0() {
		String key = "0fo0o0";
		String value = "0b0a0r";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
				System.out.println(response.getValue());
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("0b0a0r"));
	}

	@Test
	/**
	 * Repeatedly hammers the server with an amount of puts
	 * All of them have to be successful for this test to pass
	 * */
	public void test_repeated_put() {
		int amount = 1024;
		String[] keys = new String[amount];
		String[] values = new String[amount];
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < amount; i++) {
			keys[i] = "key" + (i + 1);
			values[i] = "value" + (i + 1);
			try {
				response = kvClient.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		}
	}

	/**
	 * Repeatedly hammers the server with an amount of put updates
	 * Assumes the server has a file populated with kvp pairs that have the
	 * 	format as follows; <entry key="keyX">valueX</entry> where X: 1-1024
	 * 	!!!!!  Can run test_repeated_put if you want to ensure that this test will run  !!!!!
	 * All of them have to be successful for this test to pass
	 * */
	public void test_repeated_update() {
		int amount = 1024;
		String[] keys = new String[amount];
		String value = "updated";
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < amount; i++) {
			keys[i] = "key" + (i+1);
			try {
				response = kvClient.put(keys[i], value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
		}
	}

	/**
	 * Repeatedly hammers the server with an amount of put deletes
	 * Assumes the server has a file populated with kvp pairs that have the
	 * 	format as follows; <entry key="keyX">valueX</entry> where X: 1-1024
	 * 	!!!!!  Can run test_repeated_put if you want to ensure that this test will run  !!!!!
	 * All of them have to be successful for this test to pass
	 * */
	public void test_repeated_delete() {
		int amount = 1024;
		String[] keys = new String[amount];
		String value = "null";
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < amount; i++) {
			keys[i] = "key" + (i+1);
			try {
				response = kvClient.put(keys[i], value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
		}
	}

	@Test
	/**
	 * Repeatedly hammers the server with an amount of gets from a number of threads
	 * Assumes the server has a file populated with kvp pairs that have the
	 * 	format as follows; <entry key="keyX">valueX</entry> where X: 1-1024
	 * 	!!!!!  Should run test_repeated_put if you want to ensure that this test will run  !!!!!
	 * All of them have to be successful for this test to pass
	 * */
	public void test_multiple_client_gets() {
		int numGetClients = 4;
		KVStore getClients[] = new KVStore[numGetClients];
		GetClient repeatedGetter[] = new GetClient[numGetClients];
		Thread threads[] = new Thread[numGetClients];

		KVStore kvClient = new KVStore("localhost", 8080);
		Exception ex = null;
		int i = 0;

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		if (ex == null) {
			for (i = 0; i < getClients.length; i++) {
				getClients[i] = new KVStore("localhost", 8080);
				repeatedGetter[i] = new GetClient(getClients[i], "" + i, 1023);
				threads[i] = new Thread(repeatedGetter[i]);
				threads[i].start();
			}

			for (i = 0; i < threads.length; i++) {
				System.out.println("Waiting for threads to finish");
				try {
					threads[i].join();
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}

			for(i = 0; i < repeatedGetter.length; i++){
				// any of the clients failed
				if(repeatedGetter[i].anyFailed){
					assertTrue(false);
				}
			}
			System.out.println("All threads finished");

		} else {
			System.out.println("Failed to connect run application again");
		}

		assertTrue(true);
	}
}

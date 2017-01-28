package testing;

import org.junit.Test;

import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;

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
	public void get_long_key() {
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
	public void put_long_payload() {
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
	public void put_long_key() {
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
	public void delete_get() {
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
	public void update_shorter() {
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
	public void update_longer() {
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
	public void test_repeated_put() {
		String[] keys = new String[4096];
		String[] values = new String[4096];
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < 4096; i++) {
			keys[i] = "key" + (i + 1);
			values[i] = "value" + (i + 1);
			try {
				response = kvClient.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		}

        kvClient.disconnect();
	}

	public void test_repeated_update() {
		String[] keys = new String[4096];
		String value = "updated";
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < 4096; i++) {
			keys[i] = "key" + (i+1);
			try {
				response = kvClient.put(keys[i], value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
		}
	}

	public void test_repeated_delete() {
		String[] keys = new String[4096];
		String value = "null";
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < 4096; i++) {
			keys[i] = "key" + (i+1);
			try {
				response = kvClient.put(keys[i], value);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
		}
	}

	@Test//need to figure out how to implement
	public void test_multiple_client_gets() {
		int numGetClients = 1;
		//int numPutClients = Integer.parseInt(args[1]);
		KVStore getClients[] = new KVStore[numGetClients];
		//KVStore putClients[] = new KVStore[numPutClients];
		Thread threads[] = new Thread[numGetClients/*+ numPutClients*/];

		// performance setup, store 4096 unique kvp in file
		String[] keys = new String[4096];
		String[] values = new String[4096];
		KVStore kvClient = new KVStore("localhost", 8080);
		KVMessage response = null;
		Exception ex = null;
		int i,j,k =0;

		try {
			kvClient.connect();
		} catch (Exception e){
			ex = e;
		}

		if(ex == null) {
			for (i = 0; i < getClients.length; i++) {
				getClients[i] = new KVStore("localhost", 8080);
				GetClient client = new GetClient(getClients[i], "" + i);
				threads[i] = new Thread(client);
				threads[i].start();
				k++;
			}

            /*for (j = 0; j < putClients.length; j++) {
                putClients[j] = new KVStore("localhost", 8080);
                PutClient client = new PutClient(putClients[j]);
                threads[k] = new Thread(client);
                threads[k].start();
                k++;
            }*/

			for(k = 0; k < threads.length; k++){
				System.out.println("Waiting for threads to finish");
				try {
					threads[k].join();
				} catch (Exception e){
					System.out.println(e.getMessage());
				}
			}

			System.out.println("All threads finished");

		} else {
			System.out.println("Failed to connect run application again");
		}

		assertTrue(true);
	}

}

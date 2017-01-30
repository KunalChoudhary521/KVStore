package testing;

import org.junit.Test;

import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import stress.GetClient;
import testing.perf_test1;
import java.io.FileWriter;
import java.io.PrintWriter;
import testing.perf_test_server_side;





public class AdditionalTest extends TestCase {

	// TODO add your test cases, at least 3
	private KVStore kvClient;
	public  boolean perform_test = false;
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

	/*
	Ensure that keys>20 characters cause an error
	*/
	@Test
	public void test_get_long_key() {
		String key = "The Quick Brown Fox Jumped Over The Lazy Dog";
		KVMessage response = null;
		Exception ex = null;

		try{
			response = kvClient.get(key);
		} catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() ==StatusType.GET_ERROR);
	}
	@Test
	public void test_put_long_payload() {
		String key = "lp34";
		String value = "bar";
		for (int i = 0; i<=120*1024;i++){
			value += "bar";
		}
		KVMessage response = null;
		Exception ex = null;
		try{
			response = kvClient.put(key,value);
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}
	@Test
	public void test_put_long_key() {
		String key = "The Quick Brown Foxes Jumped Over The Lazy Dog";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try{
			response = kvClient.put(key,value);
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
			System.out.println(kvClient.put(key, value).getStatus());
			System.out.println(kvClient.put(key, "null").getStatus());
			response = kvClient.get(key);
			System.out.println(response.getStatus());
			
		} catch (Exception e) {
			System.out.println("delete exeption"+e.getMessage());
			ex = e;
		}

		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
	}
	@Test
	public void test_update_shorter() {
		String key = "fool";
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
		String key = "foot";
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
		
		assertTrue(ex == null && response.getValue().equals(value));
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
	 * !!!!! Should clear contents of the file before proceeding !!!!!
	 * */
	public void test_repeated_put() {
		int amount = 1024;
		String[] keys = new String[amount];
		String[] values = new String[amount];
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < amount; i++) {
			keys[i] = "keydf" + (i + 1);
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
				response= kvClient.put(keys[i],"new");
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
		String value = "aull";
		KVMessage response = null;
		Exception ex = null;

		for(int i =0; i < amount; i++) {
			keys[i] = "key" + (i+1);
			try {
				response = kvClient.put(keys[i], value);
				response = kvClient.put(keys[i], "null");
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


	@Test
	public void test_performance(){
		if(this.perform_test == true) {
			long[][] times = new long[9][5];
			int j = 0;
			long curr_time;
			String[] tests = {"FIFO_20_80", "FIFO_50_50", "FIFO_80_20",
					"LFU_20_80", "LFU_50_50", "LFU_80_20",
					"LRU_20_80", "LRU_50_50", "LRU_80_20"};
			for (int i = 20; i < 321; i *= 2) {
				perf_test_server_side lfu = new perf_test_server_side(i, 8082, "LFU", ".\\TestFile1.txt");
				perf_test_server_side lru = new perf_test_server_side(i, 8080, "LRU", ".\\TestFile2.txt");
				perf_test_server_side fifo = new perf_test_server_side(i, 8091, "FIFO", ".\\TestFile3.txt");
				lfu.start();
				lru.start();
				fifo.start();
				perf_test1 ptest = new perf_test1(i);
				//put benchmarking statement here
				curr_time = System.nanoTime();
				ptest.test_performance_FIFO_20_80();
				times[0][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_FIFO_50_50();
				times[1][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_FIFO_80_20();
				times[2][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_LFU_20_80();
				times[3][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_LFU_50_50();
				times[4][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_LFU_80_20();
				times[5][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_LRU_20_80();
				times[6][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_LRU_50_50();
				times[7][j] = System.nanoTime() - curr_time;
				curr_time = System.nanoTime();
				ptest.test_performance_LRU_80_20();
				times[8][j] = System.nanoTime() - curr_time;
				ptest.tearDown();
				j++;
				lfu.tearDown();
				lru.tearDown();
				fifo.tearDown();
			}
			try {
				FileWriter write = new FileWriter("perf_data.csv", false);
				PrintWriter print_line = new PrintWriter(write);
				print_line.print("Test/chache size");
				for (int i = 20; i < 321; i *= 2) {
					print_line.print("," + Integer.toString(i));
				}
				for (int i = 0; i < 9; i++) {
					print_line.print("\n" + tests[i]);
					for (int k = 0; j < 5; j++) {
						print_line.print("," + times[i][k]);
					}
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

}


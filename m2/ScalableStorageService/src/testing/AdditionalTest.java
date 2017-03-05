package testing;

import client.KVStore;
import org.junit.Test;
import app_kvEcs.ECS;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;

import junit.framework.TestCase;


public class AdditionalTest extends TestCase {
	private KVStore kvClient;
	private ECS ecs;


	public void setUp() {
		/*kvClient = new KVStore("localhost", 9000);
		ecs = new ECS(true);
		ecs.initService(1,100,"LRU");

		try {
			kvClient.connect();
		} catch (Exception e) {
		}*/
	}

	public void tearDown() {
		kvClient.disconnect();
		ecs.shutDown();
	}
	// TODO add your test cases, at least 3

	//connect to 1 server perform put on different server
	@Test
	public void test_put_wrong_server() {
		ecs.start();
		String key = "key1342";
		String value = "boy";
		KVMessage res = null;
		KVMessage res2 = null;
		Exception ex = null;
		KVStore kvClient2=new KVStore("localhost", 8080);

		try{
			kvClient2.connect();
			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			res = kvClient.put(key,value);
			res2 = kvClient2.get(key);
			kvClient2.put(key,"null");
			kvClient2.disconnect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue(ex == null && res2.getValue().equals(value)&& res.getStatus()==StatusType.PUT_SUCCESS);
	}

	//connect to 1 server perform get on different server
	@Test
	public void test_get_wrong_server() {
		ecs.start();
		String key = "key1342";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		KVStore kvClient2=new KVStore("localhost", 8080);

		try{
			kvClient2.connect();
			kvClient2.put(key,value);
			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			res = kvClient.get(key);
			kvClient2.put(key,"null");
			kvClient2.disconnect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue(ex == null && res.getValue().equals(value));
	}
	//perform put on locked server
	@Test
	public void test_put_locked() {
		ecs.lockWrite("localhost",8080);
		ecs.lockWrite("localhost",9000);
		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		long start_time=0;
		long end_time=0;
		try{

			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			Runnable unlocker_obj = new unlocker(ecs);

			start_time = System.nanoTime();
			Thread th = new Thread (unlocker_obj);
			res = kvClient.put(key, value);
			end_time = System.nanoTime();
			kvClient.put(key,"null");
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && res.getStatus() ==StatusType.PUT_SUCCESS && (end_time-start_time)>=30000000);
	}
	//perform put on stopped server
	@Test
	public void test_put_stopped() {
		ecs.stop();

		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		long start_time=0;
		long end_time=0;
		try{

			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			Runnable starter_obj = new ecs_starter(ecs);

			start_time = System.nanoTime();
			Thread th = new Thread (starter_obj);
			res = kvClient.put(key, value);
			end_time = System.nanoTime();
			kvClient.put(key,"null");
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && res.getStatus() ==StatusType.PUT_SUCCESS && (end_time-start_time)>=30000000);
	}

	//perform get on stopped server
	@Test
	public void test_get_stopped() {

		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		long start_time=0;
		long end_time=0;
		try{

			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			ecs.startKVServer("localhost",8080);
			ecs.startKVServer("localhost",9000);
			kvClient.put(key,value);
			ecs.stopKVServer("localhost",8080);
			ecs.stopKVServer("localhost",9000);
			Runnable starter_obj = new ecs_starter(ecs);
			start_time = System.nanoTime();
			Thread th = new Thread (starter_obj);
			res = kvClient.get(key);
			end_time = System.nanoTime();
			kvClient.put(key,"null");
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && res.getValue() ==value&& (end_time-start_time)>=30000000);
	}

	@Test
	//perform put on wraparound
	public void test_put_wraparound() {
		ecs.start();
		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		KVMessage res2 = null;
		Exception ex = null;
		KVStore kvClient2=new KVStore("localhost", 8080);

		try{
			kvClient2.connect();
			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			res = kvClient.put(key,value);
			res2 = kvClient2.get(key);
			kvClient2.put(key,"null");
			kvClient2.disconnect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue(ex == null && res2.getValue().equals(value)&& res.getStatus()==StatusType.PUT_SUCCESS);
	}

	@Test
	//perform get on wraparound
	public void test_get_wraparound() {
		ecs.start();
		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		KVStore kvClient2=new KVStore("localhost", 8080);

		try{
			kvClient2.connect();
			kvClient2.put(key,value);
			kvClient.disconnect();
			kvClient.connect();
		}catch(Exception ex2){
			try {
				kvClient.connect();
			}
			catch(Exception ex3){
				System.out.println("connection error");
			}
		}
		try {
			res = kvClient.get(key);
			kvClient2.put(key,"null");
			kvClient2.disconnect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue(ex == null && res.getValue().equals(value));
	}

	// initKVServer
	// connect to an available server
	// if no exception is thrown, pass
	@Test
	public void testECSinitKVServer() {
		ecs.initKVServer(1, 100, "LRU", false);
		String info[] = ecs.addNode(100, "LRU").split(":");

		try {
			kvClient = new KVStore(info[0], Integer.parseInt(info[1]));
		} catch(Exception ex)
		{
			assertNull(ex);
		}
		assertTrue(true);
	}

	// initKVServer
	// add a node
	// try to connect to the node that was added
	// if no exception is thrown, you pass
	@Test
	public void testECSAddNode() {
		ecs.initKVServer(1, 100, "LRU", false);
		String info[] = ecs.addNode(100, "LRU").split(":");

		try {
			kvClient = new KVStore(info[0], Integer.parseInt(info[1]));
		} catch(Exception ex)
		{
			assertNull(ex);
		}
		assertTrue(true);
	}

	// initKVServer
	// add a node
	// connect to it to verify it was added
	// remove the node
	// try connect to the removed node
	// if an exception is thrown, pass the test
	@Test
	public void testECSRemoveNode() {
		ecs.initKVServer(1, 100, "LRU", false);
		String info[] = ecs.addNode(100, "LRU").split(":");

		try {
			kvClient = new KVStore(info[0], Integer.parseInt(info[1]));
		} catch(Exception ex)
		{
			assertNull(ex);
		}

		try{
			kvClient.disconnect();
		} catch(Exception ex)
		{
			assertNull(ex);
		}

		ecs.removeNode(info[0], Integer.parseInt(info[1]));

		try {
			kvClient = new KVStore(info[0], Integer.parseInt(info[1]));
		} catch(Exception ex)
		{
			assertNotNull(ex);
		}
	}
}


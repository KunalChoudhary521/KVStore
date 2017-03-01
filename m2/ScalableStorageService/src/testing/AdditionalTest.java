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
		kvClient = new KVStore("localhost", 9000);
		ecs = new ECS("ecs.config");
		ecs.initService(2,100,"LRU",false);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		//ecs.;
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
		ecs.stop("localhost",8080);
		ecs.stop("localhost",9000);
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
			ecs.start("localhost",8080);
			ecs.stop("localhost",9000);
			kvClient.put(key,value);
			ecs.stop("localhost",8080);
			ecs.stop("localhost",9000);
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

	@Test
	public void testStub8() {
		assertTrue(true);
	}

	@Test
	public void testStub9() {
		assertTrue(true);
	}

	@Test
	public void testStub10() {
		assertTrue(true);
	}
}


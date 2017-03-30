package testing;

import app_kvEcs.ECS;
import app_kvEcs.md5;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import org.junit.Test;
import java.lang.*;

public class AdditionalTest extends TestCase {
	private KVStore kvClient;
	private ECS ecs;
	private KVStore kvClient2;

	// TODO add your test cases, at least 3

	//connect to 1 server perform put on different server
	@Test
	public void test_put_wrong_server() {
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		String[] s2 = ecs.getRunningServers().get(1).split(":");
		String[] s3 = ecs.getRunningServers().get(2).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		ecs.unlockWrite(s2[0],Integer.parseInt(s2[1]));
		ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));
		kvClient2 = new KVStore(s2[0],Integer.parseInt(s2[1]));
		
		String key = "key6";
		String kh=" ";
		String s1h=" ";
		String s2h=" ";
		try{
			kh = md5.HashS(key);
			s1h = md5.HashS(ecs.getRunningServers().get(0));
			s2h = md5.HashS(ecs.getRunningServers().get(1));
		}
		catch(Exception h){
			System.out.println("hash\n");
		}
		boolean kh_gt_s1 =kh.compareTo(s1h)>0;
		boolean kh_gt_s2 =kh.compareTo(s2h)>0;
		boolean s1h_gt_s2h = s1h.compareTo(s2h)>0;
		boolean hashIsLessThanFs = "ffffffffffffffffffffffffffffffff".compareTo(kh) >=0;
		boolean hashIsGtThanFs = "00000000000000000000000000000000".compareTo(kh) <=0;
		String value = "boy";
		KVMessage res = null;
		KVMessage res2 = null;
		Exception ex = null;
		try {
			//connect wrong client and put, then get
			if((kh_gt_s1 && !kh_gt_s2)||(s1h_gt_s2h &&(kh_gt_s1&&hashIsLessThanFs)||(!kh_gt_s2&&hashIsGtThanFs))){
				kvClient2.connect();
				kvClient.connect();
				res = kvClient2.put(key,value);
				Thread.sleep(100000);
				res2 = kvClient.get(key);

				kvClient.put(key,"null");
				kvClient2.disconnect();
				kvClient.disconnect();
			}else{
				kvClient.connect();
				kvClient2.connect();
				res = kvClient.put(key,value);
				Thread.sleep(10000);
				res2 = kvClient2.get(key);
				kvClient2.put(key,"null");
				kvClient.disconnect();
				kvClient2.disconnect();
			}		
		}
		catch(Exception e){
			ex = e;
		}
  	      ecs.shutDown();

		assertTrue(ex == null && res2.getValue().equals(value) && res.getStatus() == StatusType.PUT_SUCCESS);


	}
	/*  kvClient2 tries to get from the wrong server, but the wrong server
        sends it the correct metadata and directs it to the right server
        to successfully retrieve the key-value pair
    */
	@Test
	public void test_get_wrong_server() {
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		String[] s2 = ecs.getRunningServers().get(1).split(":");
		String[] s3 = ecs.getRunningServers().get(2).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		ecs.unlockWrite(s2[0],Integer.parseInt(s2[1]));
		ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));

		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));
		kvClient2 = new KVStore(s2[0],Integer.parseInt(s2[1]));
		
		String key = "key6";
		String kh=" ";
		String s1h=" ";
		String s2h=" ";
		try{
			kh = md5.HashS(key);
			s1h = md5.HashS(ecs.getRunningServers().get(0));
			s2h = md5.HashS(ecs.getRunningServers().get(1));
		}
		catch(Exception h){
			System.out.println("hash\n");
		}
		boolean kh_gt_s1 =kh.compareTo(s1h)>0;
		boolean kh_gt_s2 =kh.compareTo(s2h)>0;
		boolean s1h_gt_s2h = s1h.compareTo(s2h)>0;
		boolean hashIsLessThanFs = "ffffffffffffffffffffffffffffffff".compareTo(kh) >=0;
		boolean hashIsGtThanFs = "00000000000000000000000000000000".compareTo(kh) <=0;
		String value = "boy";
		KVMessage res = null;
		KVMessage res2 = null;
		Exception ex = null;

        try {
			if((kh_gt_s1 && !kh_gt_s2)||(s1h_gt_s2h &&(kh_gt_s1&&hashIsLessThanFs)||(!kh_gt_s2&&hashIsGtThanFs))){
				kvClient.connect();
				kvClient.put(key,value);
				kvClient2.connect();
				res = kvClient2.get(key);
				kvClient.put(key,"null");
				kvClient2.disconnect();
				kvClient.disconnect();
			}
			else{
				kvClient2.connect();
				kvClient2.put(key,value);
				kvClient.connect();
				res = kvClient.get(key);
				kvClient2.put(key,"null");
				kvClient2.disconnect();
				kvClient.disconnect();
			}
        }
        catch(Exception e){
            ex = e;
        }

        assertTrue(ex == null &&  res.getStatus()==StatusType.GET_SUCCESS &&
                res.getValue().equals(value));

        ecs.shutDown();
	}
	
	//perform put on locked server
	@Test
	public void test_put_locked() {
		System.out.println("testing");
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		String[] s2 = ecs.getRunningServers().get(1).split(":");
		String[] s3 = ecs.getRunningServers().get(2).split(":");
		ecs.start();


		System.out.println("started");
		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		long start_time=0;
		long end_time=0;
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));
		try{
			kvClient.connect();
			System.out.println("connected");
		}catch(Exception ex2){
			System.out.println("connection error");
		}
		try {
			Runnable unlocker_obj = new unlocker(ecs);

			start_time = System.nanoTime();
			System.out.println("starting");
			Thread th = new Thread (unlocker_obj);
			th.run();
			System.out.println("putting");
			res = kvClient.put(key, value);
			System.out.println("put");
			end_time = System.nanoTime();
			kvClient.put(key,"null");
			kvClient.disconnect();
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && res.getStatus() ==StatusType.PUT_SUCCESS && (end_time-start_time)>=10000000);
	}
	//perform put on stopped server
	@Test
	public void test_put_stopped() {
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		String[] s2 = ecs.getRunningServers().get(1).split(":");
		String[] s3 = ecs.getRunningServers().get(2).split(":");

		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		long start_time=0;
		long end_time=0;
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));
		try{
			kvClient.connect();
		}catch(Exception ex2){
			System.out.println("connection error");
		}
		try {
			Runnable starter_obj = new ecs_starter(ecs);

			Thread th = new Thread (starter_obj);
			th.run();
			start_time = System.nanoTime();

			res = kvClient.put(key, value);
			end_time = System.nanoTime();
			kvClient.put(key,"null");
			kvClient.disconnect();
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && res.getStatus() ==StatusType.PUT_SUCCESS && (end_time-start_time)>=10000000);
	}
	
	//perform get on stopped server
	@Test
	public void test_get_stopped() {

		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		String[] s2 = ecs.getRunningServers().get(1).split(":");
		String[] s3 = ecs.getRunningServers().get(2).split(":");
		ecs.start();
		ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		ecs.unlockWrite(s2[0],Integer.parseInt(s2[1]));
		ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));
		kvClient = new KVStore(s1[0],Integer.parseInt(s1[1]));
		
		String key = "put_wrong_server";
		String value = "boy";
		KVMessage res = null;
		Exception ex = null;
		long start_time=0;
		long end_time=0;
		try{
			kvClient.connect();
		}catch(Exception ex2){
			System.out.println("connection error");
		}
		try {
			
			kvClient.put(key,value);
			Thread.sleep(100000);
			ecs.lockWrite(s1[0],Integer.parseInt(s1[1]));
			ecs.lockWrite(s2[0],Integer.parseInt(s2[1]));
			ecs.lockWrite(s3[0],Integer.parseInt(s3[1]));
			ecs.stop();
			Runnable starter_obj = new ecs_starter(ecs);

			Thread th = new Thread (starter_obj);
			th.run();
			start_time = System.nanoTime();
			res = kvClient.get(key);
			end_time = System.nanoTime();
			kvClient.put(key,"null");
			kvClient.disconnect();
		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null);
		System.out.println(res.getValue());
		assertTrue(res.getValue().equals(value));
		assertTrue((end_time-start_time)>=10000000);
	}
/*
	//UP UNTIL HERE VERIFIED BY YARON

	// initKVServer
	// connect to an available server
	// if no exception is thrown, pass
	@Test
	public void testECSinitKVServer() {
		int numOfServers = 3;
		try {
			ecs = new ECS(true);

			ecs.initKVServer(numOfServers = 3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String serverIPPort;
		String[] info;

		try {
		    for(int i = 0; i < numOfServers; i++)//try to connect to each running server
            {
                serverIPPort = ecs.getRunningServers().get(i);
                info = serverIPPort.split(":");
                kvClient = new KVStore(info[0], Integer.parseInt(info[1]));
                kvClient.connect();
                kvClient.disconnect();
            }

		} catch(Exception ex)
		{
			assertTrue(true);
		}
		ecs.shutDown();

	}

	// initKVServer
	// add a node
	// try to connect to the node that was added
	// if no exception is thrown, you pass
	@Test
	public void testECSAddNode() {
		try {
			ecs = new ECS(true);
			ecs.initKVServer(3, 10, "LRU", false);
		} catch (Exception e) {

		}
		String info[] = ecs.addNode(10, "LRU").split(":");

		try {
			kvClient = new KVStore(info[0], Integer.parseInt(info[1]));
			kvClient.connect();
			kvClient.disconnect();
		} catch(Exception ex)
		{
			assertNull(ex);
		}
        ecs.shutDown();
	}

	// initKVServer
    //Then try to remove that server

	@Test
	public void testECSRemoveNode() {
		try {
			ecs = new ECS(true);
			ecs.initKVServer(4, 10, "LRU", false);
		} catch (Exception e) {

		}
		String[] s1 = ecs.getRunningServers().get(0).split(":");
		try {
			ecs.removeNode(s1[0], Integer.parseInt(s1[1]));
		} catch (Exception e) {
		}
		ecs.shutDown();
	}*/
}

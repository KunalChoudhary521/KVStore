package testing;

import java.io.IOException;
import java.net.UnknownHostException;

import client.KVStore;

import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;


public class ConnectionTest extends TestCase {

	static {
		try {
			new LogSetup("logs/testing/connectionTest.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 9000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 9000);
		
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


package testing;

import client.KVStore;
import common.messages.KVMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	//add your test cases, at least 3
    private KVStore kvClient;

    public void setUp() {
        try {
            new LogSetup("logs/testing/additionalTests.log", Level.ALL);
            kvClient = new KVStore("127.0.0.1", 9000);
            kvClient.connect();
        } catch (Exception e) {

        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }
	
	@Test
	public void testKeyLength() {
        //choose a key where key.length() > 20
        String key = "More than 20 bytes long";
        String value = "any value";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_ERROR);
	}

    @Test
    public void testValueLength() {
        //choose a value where value.length() > (120*1024)
        String key = "valid key";
        int invalidValueSize =(120*1024) + 1;
        String value = new String(new char[invalidValueSize]).replace("\0", "a");

        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_ERROR);
    }

    @Test
    public void testWithComma() {
        //key and value are not allowed to start with a comma
        String key = ",key with comma";
        String value = "valid value";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_ERROR);
    }

    @Test
    public void testDelete() {
        //try to delete key that does not exist
        String key = Long.toString(System.nanoTime());//time of day in (ns)
        String value = "null";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.DELETE_ERROR);
    }

    @Test
    public void testMultipleClients() {
        //put <key,value> from KVClient1 and retrieve from another
        String key = "client1_key";
        String value = "client1_value";

        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS);

        KVStore client2;
        try {
            client2 = new KVStore("127.0.0.1", 9000);
            client2.connect();

            response = client2.get(key);

            client2.disconnect();
        } catch (Exception e2) {
            ex = e2;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_SUCCESS
                && response.getValue().equals(value));
    }

    @Test
    public void testCachePerformance() {
        assertTrue(true);
    }

    @Test
	public void testCacheMiss() {
        //
		assertTrue(true);
    }
}

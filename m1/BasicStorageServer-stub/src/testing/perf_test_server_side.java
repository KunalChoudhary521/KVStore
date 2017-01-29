package testing;
import app_kvServer.KVServer;
import org.junit.Rule;
import org.junit.Test;
import java.util.Random;

import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import org.junit.rules.TestRule;
import java.lang.Object;

/**
 * Created by yy on 2017-01-29.
 */
public class perf_test_server_side extends Thread{
    private KVServer serv;
    /*
    Set up a new server thread for testing performance
     */
    public perf_test_server_side(int size, int port, String strat, String file) {
        this.serv = new KVServer(port, size, strat, file, false);

    }
    /*
       kill the server thread for testing performance
        */
    public void tearDown() {
        this.serv.Stop();
    }
    /*
   run the server thread for testing performance
    */
    public void run(){
        this.serv.Run();
    }

}

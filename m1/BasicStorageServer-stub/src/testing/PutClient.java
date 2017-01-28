package testing;

import client.KVStore;

/**
 * Created by Zabeeh on 1/28/2017.
 */
public class PutClient implements Runnable{

    private KVStore client;

    public PutClient(KVStore client){
        this.client = client;
    }

    public void run(){

    }
}



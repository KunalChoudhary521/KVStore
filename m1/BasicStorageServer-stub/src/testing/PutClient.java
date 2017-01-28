package testing;

import client.KVStore;
import common.messages.KVMessage;

import java.util.Date;

/**
 * Created by Zabeeh on 1/28/2017.
 */
public class PutClient implements Runnable{


    private int minutesToRun = 1;
    private int secondsInMinutes = 15;
    private int period = minutesToRun*secondsInMinutes*1000;//1 minute
    private String id;

    private KVStore client;
    private long stopTime;
    private Utility tools;

    private int maxSize;

    public PutClient(KVStore client, String id, int maxSize){
        this.client = client;
        this.stopTime = new Date().getTime() + period;
        this.tools = new Utility();
        this.id = id;
        this.maxSize = maxSize;
        try{
            this.client.connect();
        } catch (Exception ex)
        {

        }
    }

    public void run(){
        int succesPutCounts = 0;
        long currTime = new Date().getTime();
        while(new Date().getTime() < this.stopTime){
            String randomKey = this.tools.GenerateRandomKey(maxSize);
            String randomValue = this.tools.GenerateRandomValue(maxSize);
            try {
                KVMessage result = this.client.put(randomKey, randomValue);
                if(result.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                    succesPutCounts++;
                }
            } catch (Exception ex)
            {
                System.out.println("Something went wrong when trying to put");
                break;
            }
        }

        float averagePutTime = succesPutCounts/(period/1000);
        System.out.println("Thread " + id + ": Average put time was (requests/second):" + averagePutTime);

        client.disconnect();
    }
}



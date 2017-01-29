package stress;

import client.KVStore;
import common.messages.KVMessage;

import java.util.Date;

/**
 * Created by Zabeeh on 1/28/2017.
 */
public class GetClient implements Runnable{


    private int minutesToRun = 1;
    private int secondsInMinutes = 60;
    private int period = minutesToRun*secondsInMinutes*1000;//1 minute in milliseconds
    private String id;

    private KVStore client;
    private long stopTime;
    private Utility tools;

    public boolean anyFailed = false;

    private int maxSize;

    /**
     * This is a client that repeatedly puts a kvp
     * */
    public GetClient(KVStore client, String id, int maxSize){
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
        int succesGetCounts = 0;
        long currTime = new Date().getTime();
        while(new Date().getTime() < this.stopTime){
            String randomKey = this.tools.GenerateRandomKey(maxSize);
            try {
                KVMessage result = this.client.get(randomKey);
                if(result.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                    succesGetCounts++;
                } else {
                    anyFailed = true;
                    throw new Exception("Something went wrong when trying to get");
                }
            } catch (Exception ex)
            {
                System.out.println(ex.getMessage());
                break;
            }
        }

        float averageGetTime = succesGetCounts/(period/1000);
        System.out.println("Thread" + id + ": Average get time was (requests/second):" + averageGetTime);

        this.client.disconnect();
    }
}

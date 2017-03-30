package testing;

import app_kvEcs.ECS;

import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 2017-03-01.
 */
public class ecs_starter implements Runnable{
    private final ECS ecs;
	private final String[] s1; 
	private final String[] s2; 
	private final String[] s3; 
   public ecs_starter(ECS ecs) {
        this.ecs=ecs;
           s1 = ecs.getRunningServers().get(0).split(":");
           s2 = ecs.getRunningServers().get(1).split(":");
           s3 = ecs.getRunningServers().get(2).split(":");

    }
    public void run(){
        try {
		TimeUnit.MILLISECONDS.sleep(1);


            ecs.start();
            ecs.unlockWrite(s1[0], Integer.parseInt(s1[1]));
            ecs.unlockWrite(s2[0], Integer.parseInt(s2[1]));
            ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));


        }catch(Exception e){
        }
    }
}

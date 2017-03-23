package testing;

import app_kvEcs.ECS;

import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 2017-03-01.
 */
public class unlocker implements Runnable {
    private final ECS ecs;
    public unlocker(ECS ecs) {
	System.out.println("setting ecs");
        this.ecs=ecs;
	System.out.println("set");

    }
    public void run(){
      try {
		System.out.println("trying");
		  String[] s1 = ecs.getRunningServers().get(0).split(":");
		  String[] s2 = ecs.getRunningServers().get(1).split(":");
		  String[] s3 = ecs.getRunningServers().get(2).split(":");


            	TimeUnit.MILLISECONDS.sleep(3);
		  ecs.unlockWrite(s1[0], Integer.parseInt(s1[1]));
		  ecs.unlockWrite(s2[0], Integer.parseInt(s2[1]));
		  ecs.unlockWrite(s3[0], Integer.parseInt(s3[1]));
		return;
        }catch(Exception e){
	   }
    }
}

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
		String[] s1 = this.ecs.getRunningServers().get(0).split(":");
		String[] s2 = this.ecs.getRunningServers().get(1).split(":");
            	TimeUnit.MILLISECONDS.sleep(3);
		this.ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		System.out.println("unlocked1");
		this.ecs.unlockWrite(s2[0],Integer.parseInt(s2[1]));
		System.out.println("unlocked2");
		return;
        }catch(Exception e){
	   }
    }
}

package testing;

import app_kvEcs.ECS;

import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 2017-03-01.
 */
public class ecs_starter implements Runnable{
    private final ECS ecs;
    public ecs_starter(ECS ecs) {
        this.ecs=ecs;

    }
    public void run(){
        try {
		TimeUnit.MILLISECONDS.sleep(3);
		ecs.start();
		String[] s1 = this.ecs.getRunningServers().get(0).split(":");
		String[] s2 = this.ecs.getRunningServers().get(1).split(":");
		this.ecs.unlockWrite(s1[0],Integer.parseInt(s1[1]));
		System.out.println("unlocked1");
		this.ecs.unlockWrite(s2[0],Integer.parseInt(s2[1]));

        }catch(Exception e){
        }
    }
}

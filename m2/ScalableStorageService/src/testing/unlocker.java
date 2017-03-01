package testing;

import app_kvEcs.ECS;

import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 2017-03-01.
 */
public class unlocker implements Runnable {
    private final ECS ecs;
    public unlocker(ECS ecs) {
        this.ecs=ecs;

    }
    public void run(){
        try {
            TimeUnit.MILLISECONDS.sleep(3);
            ecs.unlockWrite("localhost", 8080);
            ecs.unlockWrite("localhost", 9000);
        }catch(Exception e){
            ecs.unlockWrite("localhost", 8080);
            ecs.unlockWrite("localhost", 9000);
        }
    }
}

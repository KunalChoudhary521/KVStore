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
            ecs.startKVServer("localhost", 8080);
            ecs.startKVServer("localhost", 9000);
        }catch(Exception e){
            ecs.startKVServer("localhost", 8080);
            ecs.startKVServer("localhost", 9000);
        }
    }
}

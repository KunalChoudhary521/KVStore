package perf_test;

import app_kvEcs.ECS;
import java.util.Random;
import client.KVStore;

import java.io.File;
import java.io.PrintWriter;

/**
 * Created by yy on 2017-03-03.
 */
public class perf_tester implements Runnable{
    private long [] gets = new long[20];
    private long [] puts = new long[20];
    private KVStore [] clients;
    //private KVServer [] servers;
    private PrintWriter res;
    private PrintWriter expand_res;
    private File key_file;
    private int [] clients_to_con={1, 5, 20, 50, 100};
    private int [] servers_to_con = {1,5,10,50,100};
    private int [] cache_sizes = {5,10,15};
    private String [] strats = {"LRU", "LFU", "FIFO"};
    private ECS ecs;

    public perf_tester(){
        this.key_file = new File("keys.txt");
        try {
            this.res = new PrintWriter("res.csv");
            this.expand_res = new PrintWriter("exp_res.csv");
            expand_res.println("curr_servers,increase_by,strategy,cache_size,time elapsed");
            res.println("servers,clients,cache_size,strategy,get/put,time elapsed");
            ecs = new ECS(false);//ECS("ecs.config", false);
        }catch(Exception ex){
            System.out.println(ex);
        }
    }
    public void run(){
        int last_cs = cache_sizes[0];
        String last_st=strats[0];
        for (String st:strats){
            for (int cs:cache_sizes){
                int cur_servs=0;
                for (int serv:servers_to_con){
                    for (int cl: clients_to_con){
                        test(cl,serv,cs,st,cur_servs,last_cs, last_st);
                        cur_servs = serv;
                        last_cs = cs;
                        last_st = st;
                    }
                }
            }
        }
    }
    private void test(int clients, int num_servers, int cache_size, String strategies,int cur_servs, int last_cs, String last_strat){
        Random rg = new Random(3);
        long start_time = 0;
        long elapsed_time = 0;
        if (last_cs != cache_size){
            start_time=System.nanoTime();
            for (int i = 0; i<(cur_servs-num_servers);i++) {
                ecs.removeNode("localhost",8080+num_servers-i);
                elapsed_time = System.nanoTime() - start_time;
                String to_store = Integer.toString(cur_servs).concat(",-").concat(Integer.toString(num_servers - cur_servs)).concat(strategies).concat(Integer.toString(cache_size)).concat(Long.toString(elapsed_time));
                expand_res.println(to_store);
            }
            cur_servs = 0;
        }
        else if(last_strat != strategies){
            start_time=System.nanoTime();
            for (int i = 0; i<(cur_servs-num_servers);i++) {
                ecs.removeNode("localhost",8080+num_servers-i);
                elapsed_time = System.nanoTime() - start_time;
                String to_store = Integer.toString(cur_servs).concat(",-").concat(Integer.toString(num_servers - cur_servs)).concat(strategies).concat(Integer.toString(cache_size)).concat(Long.toString(elapsed_time));
                expand_res.println(to_store);
            }
            cur_servs = 0;
        }
        if(cur_servs<num_servers) {
            start_time=System.nanoTime();
            ecs.initService(num_servers-cur_servs,cache_size,strategies);
            ecs.start();
            elapsed_time = System.nanoTime()-start_time;
            String to_store = Integer.toString(cur_servs).concat(",").concat(Integer.toString(num_servers-cur_servs)).concat(strategies).concat(Integer.toString(cache_size)).concat(Long.toString(elapsed_time));
            expand_res.println(to_store);
        }else if(num_servers<cur_servs){
            start_time=System.nanoTime();
            for (int i = 0; i<(cur_servs-num_servers);i++) {
                ecs.removeNode("localhost",8080+num_servers-i);
                elapsed_time = System.nanoTime() - start_time;
                String to_store = Integer.toString(cur_servs).concat(",-").concat(Integer.toString(num_servers - cur_servs)).concat(strategies).concat(Integer.toString(cache_size)).concat(Long.toString(elapsed_time));
                expand_res.println(to_store);
            }
        }
        for (int i = 0; i<clients; i++){
            try {
                this.clients[i] = new KVStore("localhost", 8080 + rg.nextInt(num_servers));
                this.clients[i].connect();
            }catch(Exception ex){
                System.out.println(ex);
            }
        }
        for (int i = 0; i<20; i++){
            try {
                start_time = System.nanoTime();
                this.clients[rg.nextInt(clients)].put("key".concat(Integer.toString(rg.nextInt())), "val");
                elapsed_time=System.nanoTime()-start_time;
                String to_store = Integer.toString(num_servers).concat(",").concat(Integer.toString(clients)).concat(Integer.toString(cache_size)).concat(strategies).concat("put").concat(Long.toString(elapsed_time));
                res.println(to_store);
            }catch(Exception ex){
                System.out.println(ex);
            }
        }
        for (int i = 0; i<20; i++){
            try {
                start_time = System.nanoTime();
                this.clients[rg.nextInt(clients)].get("key".concat(Integer.toString(rg.nextInt())));
                elapsed_time=System.nanoTime()-start_time;
                String to_store = Integer.toString(num_servers).concat(",").concat(Integer.toString(clients)).concat(Integer.toString(cache_size)).concat(strategies).concat("get").concat(Long.toString(elapsed_time));
                res.println(to_store);
            }catch(Exception ex){
                System.out.println(ex);
            }
        }
    }
}

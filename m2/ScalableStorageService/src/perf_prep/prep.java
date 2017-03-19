package perf_prep;

import java.io.File;
import java.io.PrintWriter;

import client.KVStore;
import org.apache.commons.io.FileUtils;

import static org.apache.commons.io.FileUtils.readFileToString;

/**
 * Created by yy on 2017-03-02.
 */
public class prep implements Runnable {
    private File par_dir;
    private PrintWriter keys;
    private KVStore client;
    public prep(String fold){
        this.par_dir = new File(fold);
        this.client = new KVStore("localhost", 8080);
        try {
            this.keys = new PrintWriter("keys.txt");
        }catch(Exception ex){
            System.out.println(ex);
        }
    }
    public void run(){
        try {
            client.connect();
            put_files(par_dir, "");
            client.disconnect();
        }catch(Exception ex){
            System.out.println(ex);
        }
    }
    private void put_files(File dir, String key_prefix){

        for (File f : dir.listFiles()){
            try {
                if (f.isDirectory()) {
                    put_files(f, key_prefix.concat(f.getName()));
                } else {
                    String key = key_prefix.concat(f.getName());
                    if (key.length()>=20){
                        key=key.substring(key.length()-20,key.length());
                    }
                    String value = readFileToString(f);
                    System.out.println(key);
                    value=value.trim();
                    client.put(key,value);
                    keys.println(key);

                }
            }catch(Exception ex){
                System.out.println(f.getAbsolutePath());
                System.out.println(ex);
            }

        }
    }
}

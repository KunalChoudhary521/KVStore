package perf_test;

import java.io.File;

/**
 * Created by yy on 2017-03-02.
 */
public class prep_Runner {
    public static void main(String[] args){
        prep p = new prep("C:\\Users\\yy\\Downloads\\enron_mail_20150507\\maildir");
        p.run();
    }
}

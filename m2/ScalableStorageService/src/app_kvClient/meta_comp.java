package app_kvClient;
import java.util.*;
/**
 * Created by yy on 2017-03-18.
 */
public class meta_comp implements Comparator<HashMap>{
    public meta_comp(){}
    public int compare(HashMap H1, HashMap H2){
        return ((String)H1.get("start")).compareTo((String)H2.get("start"));
    }
}
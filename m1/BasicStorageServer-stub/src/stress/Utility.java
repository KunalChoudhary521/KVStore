package stress;

import java.util.Random;

/**
 * Created by Zabeeh on 1/28/2017.
 */
public class Utility {

    public Utility(){

    }

    public String GenerateRandomKey(int maxSize){
        int kn = new Random().nextInt(maxSize+1) + 1;
        return "key"+kn;
    }

    public String GenerateRandomValue(int maxSize) {
        int vn = new Random().nextInt(maxSize+1) + 1;
        return ""+vn;
    }
}

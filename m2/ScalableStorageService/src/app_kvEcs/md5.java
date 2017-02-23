package app_kvEcs;

import java.security.*;
import java.math.*;

public class md5 
{
    /*  Don't create an instance of md5 class, just call
        this function as: md5.getHash("abc")
    */
    public static String getHash(String s)
    {
       try
       {
           //s can be a KV-pair or KVServer's <IP>:<Port>
           MessageDigest m = MessageDigest.getInstance("MD5");
           m.update(s.getBytes(), 0, s.length());
           //System.out.println("MD5: "+ new BigInteger(1,m.digest()).toString(16));
           BigInteger a = new BigInteger(1, m.digest());
           return (new BigInteger(1, m.digest()).toString(16));
       }
       catch(Exception ex)
       {
           System.out.println("--------md5 of " + s + " failed--------");
           return null;
       }
   }

    public static void main(String[] args)
    {
        String input = "192.168.0.1:8000";
        String hexStr = md5.getHash(input);
        System.out.println("md5 Hash in hex-string " +  hexStr);

        BigInteger hash = new BigInteger(hexStr,16);
        System.out.println("md5 Hash in BigInteger " +  hash);
    }
}
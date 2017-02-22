package app_kvEcs;

import java.security.*;
import java.math.*;

public class md5 
{
   public static void main(String args[]) throws Exception
   {
      String s="192.168.0.1:9001";//example string
      MessageDigest m=MessageDigest.getInstance("MD5");
      m.update(s.getBytes(),0,s.length());
      System.out.println("MD5: "+ new BigInteger(1,m.digest()).toString(16));
   }
}
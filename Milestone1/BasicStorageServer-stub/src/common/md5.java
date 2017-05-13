package common;

import java.security.*;
import java.math.*;

public class md5 
{
   public static BigInteger HashInBI(String s) throws NoSuchAlgorithmException
   {
      MessageDigest m= MessageDigest.getInstance("MD5");
      m.update(s.getBytes(),0,s.length());
      return new BigInteger(1,m.digest());
   }

   public static String HashInStr(String s) throws NoSuchAlgorithmException
   {
      MessageDigest m= MessageDigest.getInstance("MD5");
      m.update(s.getBytes(),0,s.length());
      return new BigInteger(1,m.digest()).toString(16);
   }

}
package common;

import java.security.*;
import java.math.*;

public class md5 
{
   public static BigInteger HashBI(String s) throws Exception
   {
      MessageDigest m= MessageDigest.getInstance("MD5");
      m.update(s.getBytes(),0,s.length());
      return new BigInteger(1,m.digest());
   }

   public static String HashS(String s) throws Exception
   {
      MessageDigest m= MessageDigest.getInstance("MD5");
      m.update(s.getBytes(),0,s.length());
      return new BigInteger(1,m.digest()).toString(16);
   }

}
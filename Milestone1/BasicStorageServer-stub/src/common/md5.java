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
      String hashStr = new BigInteger(1,m.digest()).toString(16);

      /*    If the hash String is NOT 32 bytes in length,
            then pad it with zeros from the left
       */
      if(hashStr.length() < 32)
      {
          int padding = 32 - hashStr.length();
          String pad = new String(new char[padding]).replace("\0", "0");
          hashStr = pad + hashStr;
      }
      return hashStr;
   }

}
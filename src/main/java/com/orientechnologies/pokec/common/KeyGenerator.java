package com.orientechnologies.pokec.common;

public class KeyGenerator {
  public static String generateKey(ZipfianGenerator generator, int keyCount) {
    long ret = generator.nextLong();
    long keyVal = FNVHash.FNVhash64(ret % keyCount);
    final String key = "key" + keyVal;
    return key;
  }
}

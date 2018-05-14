package com.orientechnologies.pokec;

class FNVHash {
  private static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
  private static final long FNV_prime_64        = 1099511628211L;

  static long FNVhash64(long val) {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hashval = FNV_offset_basis_64;

    for (int i = 0; i < 8; i++) {
      long octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_prime_64;
    }

    return Math.abs(hashval);
  }
}

package com.alibaba.graphscope.ds;

import java.lang.Double;
import java.lang.Long;

public class GrapeNbr_cxx_0xa3660297Factory implements GrapeNbr.Factory<Long, Double> {
  public static final GrapeNbr.Factory<Long, Double> INSTANCE;

  static {
    INSTANCE = new GrapeNbr_cxx_0xa3660297Factory();
  }

  public GrapeNbr_cxx_0xa3660297Factory() {
  }

  public GrapeNbr<Long, Double> create() {
    return new GrapeNbr_cxx_0xa3660297(GrapeNbr_cxx_0xa3660297.nativeCreateFactory0());
  }

  public GrapeNbr<Long, Double> create(Long arg0) {
    return new GrapeNbr_cxx_0xa3660297(GrapeNbr_cxx_0xa3660297.nativeCreateFactory1(arg0));
  }

  public GrapeNbr<Long, Double> create(Long arg0, Double arg1) {
    return new GrapeNbr_cxx_0xa3660297(GrapeNbr_cxx_0xa3660297.nativeCreateFactory2(arg0, arg1));
  }
}

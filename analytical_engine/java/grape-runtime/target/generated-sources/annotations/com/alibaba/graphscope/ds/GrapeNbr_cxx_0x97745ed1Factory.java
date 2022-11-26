package com.alibaba.graphscope.ds;

import java.lang.Integer;
import java.lang.Long;

public class GrapeNbr_cxx_0x97745ed1Factory implements GrapeNbr.Factory<Long, Integer> {
  public static final GrapeNbr.Factory<Long, Integer> INSTANCE;

  static {
    INSTANCE = new GrapeNbr_cxx_0x97745ed1Factory();
  }

  public GrapeNbr_cxx_0x97745ed1Factory() {
  }

  public GrapeNbr<Long, Integer> create() {
    return new GrapeNbr_cxx_0x97745ed1(GrapeNbr_cxx_0x97745ed1.nativeCreateFactory0());
  }

  public GrapeNbr<Long, Integer> create(Long arg0) {
    return new GrapeNbr_cxx_0x97745ed1(GrapeNbr_cxx_0x97745ed1.nativeCreateFactory1(arg0));
  }

  public GrapeNbr<Long, Integer> create(Long arg0, Integer arg1) {
    return new GrapeNbr_cxx_0x97745ed1(GrapeNbr_cxx_0x97745ed1.nativeCreateFactory2(arg0, arg1));
  }
}

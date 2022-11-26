package com.alibaba.graphscope.ds;

import java.lang.Long;

public class GrapeNbr_cxx_0x979f8e12Factory implements GrapeNbr.Factory<Long, Long> {
  public static final GrapeNbr.Factory<Long, Long> INSTANCE;

  static {
    INSTANCE = new GrapeNbr_cxx_0x979f8e12Factory();
  }

  public GrapeNbr_cxx_0x979f8e12Factory() {
  }

  public GrapeNbr<Long, Long> create() {
    return new GrapeNbr_cxx_0x979f8e12(GrapeNbr_cxx_0x979f8e12.nativeCreateFactory0());
  }

  public GrapeNbr<Long, Long> create(Long arg0) {
    return new GrapeNbr_cxx_0x979f8e12(GrapeNbr_cxx_0x979f8e12.nativeCreateFactory1(arg0));
  }

  public GrapeNbr<Long, Long> create(Long arg0, Long arg1) {
    return new GrapeNbr_cxx_0x979f8e12(GrapeNbr_cxx_0x979f8e12.nativeCreateFactory2(arg0, arg1));
  }
}

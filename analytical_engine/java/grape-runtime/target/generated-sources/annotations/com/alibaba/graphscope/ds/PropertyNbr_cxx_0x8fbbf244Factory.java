package com.alibaba.graphscope.ds;

import java.lang.Long;

public class PropertyNbr_cxx_0x8fbbf244Factory implements PropertyNbr.Factory<Long> {
  public static final PropertyNbr.Factory<Long> INSTANCE;

  static {
    INSTANCE = new PropertyNbr_cxx_0x8fbbf244Factory();
  }

  public PropertyNbr_cxx_0x8fbbf244Factory() {
  }

  public PropertyNbr<Long> create() {
    return new PropertyNbr_cxx_0x8fbbf244(PropertyNbr_cxx_0x8fbbf244.nativeCreateFactory0());
  }
}

package com.alibaba.graphscope.arrow.array;

import java.lang.Long;

public class PrimitiveArrowArrayBuilder_cxx_0x10daeab4Factory implements PrimitiveArrowArrayBuilder.Factory<Long> {
  public static final PrimitiveArrowArrayBuilder.Factory<Long> INSTANCE;

  static {
    INSTANCE = new PrimitiveArrowArrayBuilder_cxx_0x10daeab4Factory();
  }

  public PrimitiveArrowArrayBuilder_cxx_0x10daeab4Factory() {
  }

  public PrimitiveArrowArrayBuilder<Long> create() {
    return new PrimitiveArrowArrayBuilder_cxx_0x10daeab4(PrimitiveArrowArrayBuilder_cxx_0x10daeab4.nativeCreateFactory0());
  }
}

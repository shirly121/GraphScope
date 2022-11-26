package com.alibaba.graphscope.arrow.array;

import java.lang.Long;

public class PrimitiveArrowArrayBuilder_cxx_0x10471a19Factory implements PrimitiveArrowArrayBuilder.Factory<Long> {
  public static final PrimitiveArrowArrayBuilder.Factory<Long> INSTANCE;

  static {
    INSTANCE = new PrimitiveArrowArrayBuilder_cxx_0x10471a19Factory();
  }

  public PrimitiveArrowArrayBuilder_cxx_0x10471a19Factory() {
  }

  public PrimitiveArrowArrayBuilder<Long> create() {
    return new PrimitiveArrowArrayBuilder_cxx_0x10471a19(PrimitiveArrowArrayBuilder_cxx_0x10471a19.nativeCreateFactory0());
  }
}

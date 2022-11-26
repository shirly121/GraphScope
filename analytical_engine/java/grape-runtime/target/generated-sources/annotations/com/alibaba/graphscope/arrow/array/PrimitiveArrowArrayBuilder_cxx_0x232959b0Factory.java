package com.alibaba.graphscope.arrow.array;

import java.lang.Double;

public class PrimitiveArrowArrayBuilder_cxx_0x232959b0Factory implements PrimitiveArrowArrayBuilder.Factory<Double> {
  public static final PrimitiveArrowArrayBuilder.Factory<Double> INSTANCE;

  static {
    INSTANCE = new PrimitiveArrowArrayBuilder_cxx_0x232959b0Factory();
  }

  public PrimitiveArrowArrayBuilder_cxx_0x232959b0Factory() {
  }

  public PrimitiveArrowArrayBuilder<Double> create() {
    return new PrimitiveArrowArrayBuilder_cxx_0x232959b0(PrimitiveArrowArrayBuilder_cxx_0x232959b0.nativeCreateFactory0());
  }
}

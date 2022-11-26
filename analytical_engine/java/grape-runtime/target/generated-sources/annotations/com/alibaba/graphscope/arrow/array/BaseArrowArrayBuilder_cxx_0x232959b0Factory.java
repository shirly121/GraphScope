package com.alibaba.graphscope.arrow.array;

import java.lang.Double;

public class BaseArrowArrayBuilder_cxx_0x232959b0Factory implements BaseArrowArrayBuilder.Factory<Double> {
  public static final BaseArrowArrayBuilder.Factory<Double> INSTANCE;

  static {
    INSTANCE = new BaseArrowArrayBuilder_cxx_0x232959b0Factory();
  }

  public BaseArrowArrayBuilder_cxx_0x232959b0Factory() {
  }

  public BaseArrowArrayBuilder<Double> create() {
    return new BaseArrowArrayBuilder_cxx_0x232959b0(BaseArrowArrayBuilder_cxx_0x232959b0.nativeCreateFactory0());
  }
}

package com.alibaba.graphscope.arrow.array;

import java.lang.Long;

public class BaseArrowArrayBuilder_cxx_0x10471a19Factory implements BaseArrowArrayBuilder.Factory<Long> {
  public static final BaseArrowArrayBuilder.Factory<Long> INSTANCE;

  static {
    INSTANCE = new BaseArrowArrayBuilder_cxx_0x10471a19Factory();
  }

  public BaseArrowArrayBuilder_cxx_0x10471a19Factory() {
  }

  public BaseArrowArrayBuilder<Long> create() {
    return new BaseArrowArrayBuilder_cxx_0x10471a19(BaseArrowArrayBuilder_cxx_0x10471a19.nativeCreateFactory0());
  }
}

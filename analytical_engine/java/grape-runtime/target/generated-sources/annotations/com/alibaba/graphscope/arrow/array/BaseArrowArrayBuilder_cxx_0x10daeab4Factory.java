package com.alibaba.graphscope.arrow.array;

import java.lang.Long;

public class BaseArrowArrayBuilder_cxx_0x10daeab4Factory implements BaseArrowArrayBuilder.Factory<Long> {
  public static final BaseArrowArrayBuilder.Factory<Long> INSTANCE;

  static {
    INSTANCE = new BaseArrowArrayBuilder_cxx_0x10daeab4Factory();
  }

  public BaseArrowArrayBuilder_cxx_0x10daeab4Factory() {
  }

  public BaseArrowArrayBuilder<Long> create() {
    return new BaseArrowArrayBuilder_cxx_0x10daeab4(BaseArrowArrayBuilder_cxx_0x10daeab4.nativeCreateFactory0());
  }
}

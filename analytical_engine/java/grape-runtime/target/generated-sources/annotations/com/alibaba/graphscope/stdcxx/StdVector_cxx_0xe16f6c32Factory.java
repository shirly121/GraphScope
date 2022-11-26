package com.alibaba.graphscope.stdcxx;

import java.lang.Long;

public class StdVector_cxx_0xe16f6c32Factory implements StdVector.Factory<Long> {
  public static final StdVector.Factory<Long> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0xe16f6c32Factory();
  }

  public StdVector_cxx_0xe16f6c32Factory() {
  }

  public StdVector<Long> create() {
    return new StdVector_cxx_0xe16f6c32(StdVector_cxx_0xe16f6c32.nativeCreateFactory0());
  }
}

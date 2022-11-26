package com.alibaba.graphscope.ds;

import java.lang.Long;

public class PrimitiveTypedArray_cxx_0xb7aa6c0aFactory implements PrimitiveTypedArray.Factory<Long> {
  public static final PrimitiveTypedArray.Factory<Long> INSTANCE;

  static {
    INSTANCE = new PrimitiveTypedArray_cxx_0xb7aa6c0aFactory();
  }

  public PrimitiveTypedArray_cxx_0xb7aa6c0aFactory() {
  }

  public PrimitiveTypedArray<Long> create() {
    return new PrimitiveTypedArray_cxx_0xb7aa6c0a(PrimitiveTypedArray_cxx_0xb7aa6c0a.nativeCreateFactory0());
  }
}

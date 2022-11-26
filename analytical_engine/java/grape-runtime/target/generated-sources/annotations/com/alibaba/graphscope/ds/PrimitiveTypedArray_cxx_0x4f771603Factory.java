package com.alibaba.graphscope.ds;

import java.lang.Long;

public class PrimitiveTypedArray_cxx_0x4f771603Factory implements PrimitiveTypedArray.Factory<Long> {
  public static final PrimitiveTypedArray.Factory<Long> INSTANCE;

  static {
    INSTANCE = new PrimitiveTypedArray_cxx_0x4f771603Factory();
  }

  public PrimitiveTypedArray_cxx_0x4f771603Factory() {
  }

  public PrimitiveTypedArray<Long> create() {
    return new PrimitiveTypedArray_cxx_0x4f771603(PrimitiveTypedArray_cxx_0x4f771603.nativeCreateFactory0());
  }
}

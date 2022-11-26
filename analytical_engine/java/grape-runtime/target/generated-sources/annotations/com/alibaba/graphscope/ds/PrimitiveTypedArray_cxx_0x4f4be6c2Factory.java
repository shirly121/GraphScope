package com.alibaba.graphscope.ds;

import java.lang.Integer;

public class PrimitiveTypedArray_cxx_0x4f4be6c2Factory implements PrimitiveTypedArray.Factory<Integer> {
  public static final PrimitiveTypedArray.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new PrimitiveTypedArray_cxx_0x4f4be6c2Factory();
  }

  public PrimitiveTypedArray_cxx_0x4f4be6c2Factory() {
  }

  public PrimitiveTypedArray<Integer> create() {
    return new PrimitiveTypedArray_cxx_0x4f4be6c2(PrimitiveTypedArray_cxx_0x4f4be6c2.nativeCreateFactory0());
  }
}

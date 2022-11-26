package com.alibaba.graphscope.ds;

import java.lang.Double;

public class PrimitiveTypedArray_cxx_0x2d753886Factory implements PrimitiveTypedArray.Factory<Double> {
  public static final PrimitiveTypedArray.Factory<Double> INSTANCE;

  static {
    INSTANCE = new PrimitiveTypedArray_cxx_0x2d753886Factory();
  }

  public PrimitiveTypedArray_cxx_0x2d753886Factory() {
  }

  public PrimitiveTypedArray<Double> create() {
    return new PrimitiveTypedArray_cxx_0x2d753886(PrimitiveTypedArray_cxx_0x2d753886.nativeCreateFactory0());
  }
}

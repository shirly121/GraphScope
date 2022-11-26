package com.alibaba.graphscope.arrow.array;

import java.lang.Integer;

public class PrimitiveArrowArrayBuilder_cxx_0x101bead8Factory implements PrimitiveArrowArrayBuilder.Factory<Integer> {
  public static final PrimitiveArrowArrayBuilder.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new PrimitiveArrowArrayBuilder_cxx_0x101bead8Factory();
  }

  public PrimitiveArrowArrayBuilder_cxx_0x101bead8Factory() {
  }

  public PrimitiveArrowArrayBuilder<Integer> create() {
    return new PrimitiveArrowArrayBuilder_cxx_0x101bead8(PrimitiveArrowArrayBuilder_cxx_0x101bead8.nativeCreateFactory0());
  }
}

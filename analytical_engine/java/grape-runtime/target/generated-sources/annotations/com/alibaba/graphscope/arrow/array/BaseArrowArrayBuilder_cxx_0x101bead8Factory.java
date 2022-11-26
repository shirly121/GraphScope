package com.alibaba.graphscope.arrow.array;

import java.lang.Integer;

public class BaseArrowArrayBuilder_cxx_0x101bead8Factory implements BaseArrowArrayBuilder.Factory<Integer> {
  public static final BaseArrowArrayBuilder.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new BaseArrowArrayBuilder_cxx_0x101bead8Factory();
  }

  public BaseArrowArrayBuilder_cxx_0x101bead8Factory() {
  }

  public BaseArrowArrayBuilder<Integer> create() {
    return new BaseArrowArrayBuilder_cxx_0x101bead8(BaseArrowArrayBuilder_cxx_0x101bead8.nativeCreateFactory0());
  }
}

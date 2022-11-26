package com.alibaba.graphscope.ds;

import java.lang.Long;

public class VertexRange_cxx_0x528b3543Factory implements VertexRange.Factory<Long> {
  public static final VertexRange.Factory<Long> INSTANCE;

  static {
    INSTANCE = new VertexRange_cxx_0x528b3543Factory();
  }

  public VertexRange_cxx_0x528b3543Factory() {
  }

  public VertexRange<Long> create() {
    return new VertexRange_cxx_0x528b3543(VertexRange_cxx_0x528b3543.nativeCreateFactory0());
  }

  public VertexRange<Long> create(Long arg0, Long arg1) {
    return new VertexRange_cxx_0x528b3543(VertexRange_cxx_0x528b3543.nativeCreateFactory1(arg0, arg1));
  }
}

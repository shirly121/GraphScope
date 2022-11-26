package com.alibaba.graphscope.ds;

import java.lang.Integer;

public class VertexRange_cxx_0x52600602Factory implements VertexRange.Factory<Integer> {
  public static final VertexRange.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new VertexRange_cxx_0x52600602Factory();
  }

  public VertexRange_cxx_0x52600602Factory() {
  }

  public VertexRange<Integer> create() {
    return new VertexRange_cxx_0x52600602(VertexRange_cxx_0x52600602.nativeCreateFactory0());
  }

  public VertexRange<Integer> create(Integer arg0, Integer arg1) {
    return new VertexRange_cxx_0x52600602(VertexRange_cxx_0x52600602.nativeCreateFactory1(arg0, arg1));
  }
}

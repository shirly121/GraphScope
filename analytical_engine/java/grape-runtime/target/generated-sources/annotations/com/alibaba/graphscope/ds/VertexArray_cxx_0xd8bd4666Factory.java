package com.alibaba.graphscope.ds;

import java.lang.Long;

public class VertexArray_cxx_0xd8bd4666Factory implements VertexArray.Factory<Long, Long> {
  public static final VertexArray.Factory<Long, Long> INSTANCE;

  static {
    INSTANCE = new VertexArray_cxx_0xd8bd4666Factory();
  }

  public VertexArray_cxx_0xd8bd4666Factory() {
  }

  public VertexArray<Long, Long> create() {
    return new VertexArray_cxx_0xd8bd4666(VertexArray_cxx_0xd8bd4666.nativeCreateFactory0());
  }
}

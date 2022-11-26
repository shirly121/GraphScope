package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0xf3953ab8Factory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Integer, Long>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Integer, Long>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0xf3953ab8Factory();
  }

  public FFIVertexDataContext_cxx_0xf3953ab8Factory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Integer, Long>, Integer> create(
      ArrowProjectedFragment<Long, Long, Integer, Long> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0xf3953ab8(FFIVertexDataContext_cxx_0xf3953ab8.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

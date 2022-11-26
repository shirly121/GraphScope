package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x70f5d0d9Factory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Integer, Integer>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Integer, Integer>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x70f5d0d9Factory();
  }

  public FFIVertexDataContext_cxx_0x70f5d0d9Factory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Integer, Integer>, Integer> create(
      ArrowProjectedFragment<Long, Long, Integer, Integer> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x70f5d0d9(FFIVertexDataContext_cxx_0x70f5d0d9.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

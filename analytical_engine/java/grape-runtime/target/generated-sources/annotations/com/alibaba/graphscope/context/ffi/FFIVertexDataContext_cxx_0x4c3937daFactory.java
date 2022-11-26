package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x4c3937daFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Long>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Long>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x4c3937daFactory();
  }

  public FFIVertexDataContext_cxx_0x4c3937daFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Double, Long>, Integer> create(
      ArrowProjectedFragment<Long, Long, Double, Long> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x4c3937da(FFIVertexDataContext_cxx_0x4c3937da.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x5b2af72eFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Double>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Double>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x5b2af72eFactory();
  }

  public FFIVertexDataContext_cxx_0x5b2af72eFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Long, Double>, Integer> create(
      ArrowProjectedFragment<Long, Long, Long, Double> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x5b2af72e(FFIVertexDataContext_cxx_0x5b2af72e.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

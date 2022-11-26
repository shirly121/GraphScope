package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0xc999cdfbFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Integer>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Integer>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0xc999cdfbFactory();
  }

  public FFIVertexDataContext_cxx_0xc999cdfbFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Double, Integer>, Integer> create(
      ArrowProjectedFragment<Long, Long, Double, Integer> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0xc999cdfb(FFIVertexDataContext_cxx_0xc999cdfb.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

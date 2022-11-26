package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x661c69edFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Integer, Double>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Integer, Double>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x661c69edFactory();
  }

  public FFIVertexDataContext_cxx_0x661c69edFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Integer, Double>, Integer> create(
      ArrowProjectedFragment<Long, Long, Integer, Double> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x661c69ed(FFIVertexDataContext_cxx_0x661c69ed.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

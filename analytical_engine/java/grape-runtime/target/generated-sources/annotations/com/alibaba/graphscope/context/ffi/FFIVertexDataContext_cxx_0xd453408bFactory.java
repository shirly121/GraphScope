package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0xd453408bFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Double>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Double>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0xd453408bFactory();
  }

  public FFIVertexDataContext_cxx_0xd453408bFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Double, Double>, Integer> create(
      ArrowProjectedFragment<Long, Long, Double, Double> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0xd453408b(FFIVertexDataContext_cxx_0xd453408b.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

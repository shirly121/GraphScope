package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x7a651b11Factory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Long>, Double> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Long>, Double> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x7a651b11Factory();
  }

  public FFIVertexDataContext_cxx_0x7a651b11Factory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Long, Long>, Double> create(
      ArrowProjectedFragment<Long, Long, Long, Long> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x7a651b11(FFIVertexDataContext_cxx_0x7a651b11.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x4c64671bFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Long>, Long> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Double, Long>, Long> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x4c64671bFactory();
  }

  public FFIVertexDataContext_cxx_0x4c64671bFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Double, Long>, Long> create(
      ArrowProjectedFragment<Long, Long, Double, Long> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x4c64671b(FFIVertexDataContext_cxx_0x4c64671b.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

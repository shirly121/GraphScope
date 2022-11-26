package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0xa0585597Factory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Long>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Long>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0xa0585597Factory();
  }

  public FFIVertexDataContext_cxx_0xa0585597Factory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Long, Long>, Integer> create(
      ArrowProjectedFragment<Long, Long, Long, Long> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0xa0585597(FFIVertexDataContext_cxx_0xa0585597.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

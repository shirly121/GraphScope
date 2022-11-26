package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0xa08384d8Factory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Long>, Long> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Long>, Long> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0xa08384d8Factory();
  }

  public FFIVertexDataContext_cxx_0xa08384d8Factory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Long, Long>, Long> create(
      ArrowProjectedFragment<Long, Long, Long, Long> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0xa08384d8(FFIVertexDataContext_cxx_0xa08384d8.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

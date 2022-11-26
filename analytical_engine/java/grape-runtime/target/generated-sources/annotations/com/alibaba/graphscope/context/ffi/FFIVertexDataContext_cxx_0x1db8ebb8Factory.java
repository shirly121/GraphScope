package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Integer;
import java.lang.Long;

public class FFIVertexDataContext_cxx_0x1db8ebb8Factory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Integer>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, Long, Integer>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0x1db8ebb8Factory();
  }

  public FFIVertexDataContext_cxx_0x1db8ebb8Factory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Long, Integer>, Integer> create(
      ArrowProjectedFragment<Long, Long, Long, Integer> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0x1db8ebb8(FFIVertexDataContext_cxx_0x1db8ebb8.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

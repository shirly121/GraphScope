package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Integer;
import java.lang.Long;
import java.lang.String;

public class FFIVertexDataContext_cxx_0xb5cb6a3bFactory implements FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, String, String>, Integer> {
  public static final FFIVertexDataContext.Factory<ArrowProjectedFragment<Long, Long, String, String>, Integer> INSTANCE;

  static {
    INSTANCE = new FFIVertexDataContext_cxx_0xb5cb6a3bFactory();
  }

  public FFIVertexDataContext_cxx_0xb5cb6a3bFactory() {
  }

  public FFIVertexDataContext<ArrowProjectedFragment<Long, Long, String, String>, Integer> create(
      ArrowProjectedFragment<Long, Long, String, String> arg0, boolean arg1) {
    return new FFIVertexDataContext_cxx_0xb5cb6a3b(FFIVertexDataContext_cxx_0xb5cb6a3b.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

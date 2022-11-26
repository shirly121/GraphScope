package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowFragment;
import java.lang.Long;

public class FFILabeledVertexDataContext_cxx_0xd1aafb66Factory implements FFILabeledVertexDataContext.Factory<ArrowFragment<Long>, Long> {
  public static final FFILabeledVertexDataContext.Factory<ArrowFragment<Long>, Long> INSTANCE;

  static {
    INSTANCE = new FFILabeledVertexDataContext_cxx_0xd1aafb66Factory();
  }

  public FFILabeledVertexDataContext_cxx_0xd1aafb66Factory() {
  }

  public FFILabeledVertexDataContext<ArrowFragment<Long>, Long> create(ArrowFragment<Long> arg0,
      boolean arg1) {
    return new FFILabeledVertexDataContext_cxx_0xd1aafb66(FFILabeledVertexDataContext_cxx_0xd1aafb66.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

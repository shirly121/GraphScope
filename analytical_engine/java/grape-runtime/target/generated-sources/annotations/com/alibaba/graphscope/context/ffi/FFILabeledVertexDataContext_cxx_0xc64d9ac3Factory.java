package com.alibaba.graphscope.context.ffi;

import com.alibaba.graphscope.fragment.ArrowFragment;
import java.lang.Double;
import java.lang.Long;

public class FFILabeledVertexDataContext_cxx_0xc64d9ac3Factory implements FFILabeledVertexDataContext.Factory<ArrowFragment<Long>, Double> {
  public static final FFILabeledVertexDataContext.Factory<ArrowFragment<Long>, Double> INSTANCE;

  static {
    INSTANCE = new FFILabeledVertexDataContext_cxx_0xc64d9ac3Factory();
  }

  public FFILabeledVertexDataContext_cxx_0xc64d9ac3Factory() {
  }

  public FFILabeledVertexDataContext<ArrowFragment<Long>, Double> create(ArrowFragment<Long> arg0,
      boolean arg1) {
    return new FFILabeledVertexDataContext_cxx_0xc64d9ac3(FFILabeledVertexDataContext_cxx_0xc64d9ac3.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.stdcxx.StdVector;
import java.lang.Long;

public class GraphXRawDataBuilder_cxx_0xa6c829deFactory implements GraphXRawDataBuilder.Factory<Long, Long, Long, Long> {
  public static final GraphXRawDataBuilder.Factory<Long, Long, Long, Long> INSTANCE;

  static {
    INSTANCE = new GraphXRawDataBuilder_cxx_0xa6c829deFactory();
  }

  public GraphXRawDataBuilder_cxx_0xa6c829deFactory() {
  }

  public GraphXRawDataBuilder<Long, Long, Long, Long> create(VineyardClient arg0,
      StdVector<Long> arg1, StdVector<Long> arg2, StdVector<Long> arg3, StdVector<Long> arg4,
      StdVector<Long> arg5) {
    return new GraphXRawDataBuilder_cxx_0xa6c829de(GraphXRawDataBuilder_cxx_0xa6c829de.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address, ((com.alibaba.fastffi.FFIPointerImpl) arg3).address, ((com.alibaba.fastffi.FFIPointerImpl) arg4).address, ((com.alibaba.fastffi.FFIPointerImpl) arg5).address));
  }
}

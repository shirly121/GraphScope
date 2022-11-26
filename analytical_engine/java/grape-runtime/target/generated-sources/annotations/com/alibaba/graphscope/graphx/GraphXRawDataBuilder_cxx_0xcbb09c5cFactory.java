package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.stdcxx.StdVector;
import java.lang.Integer;
import java.lang.Long;

public class GraphXRawDataBuilder_cxx_0xcbb09c5cFactory implements GraphXRawDataBuilder.Factory<Long, Long, Integer, Integer> {
  public static final GraphXRawDataBuilder.Factory<Long, Long, Integer, Integer> INSTANCE;

  static {
    INSTANCE = new GraphXRawDataBuilder_cxx_0xcbb09c5cFactory();
  }

  public GraphXRawDataBuilder_cxx_0xcbb09c5cFactory() {
  }

  public GraphXRawDataBuilder<Long, Long, Integer, Integer> create(VineyardClient arg0,
      StdVector<Long> arg1, StdVector<Integer> arg2, StdVector<Long> arg3, StdVector<Long> arg4,
      StdVector<Integer> arg5) {
    return new GraphXRawDataBuilder_cxx_0xcbb09c5c(GraphXRawDataBuilder_cxx_0xcbb09c5c.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address, ((com.alibaba.fastffi.FFIPointerImpl) arg3).address, ((com.alibaba.fastffi.FFIPointerImpl) arg4).address, ((com.alibaba.fastffi.FFIPointerImpl) arg5).address));
  }
}

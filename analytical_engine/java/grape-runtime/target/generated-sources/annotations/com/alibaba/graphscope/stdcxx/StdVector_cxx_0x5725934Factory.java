package com.alibaba.graphscope.stdcxx;

import com.alibaba.graphscope.ds.GSVertexArray;
import java.lang.Long;

public class StdVector_cxx_0x5725934Factory implements StdVector.Factory<GSVertexArray<Long>> {
  public static final StdVector.Factory<GSVertexArray<Long>> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0x5725934Factory();
  }

  public StdVector_cxx_0x5725934Factory() {
  }

  public StdVector<GSVertexArray<Long>> create() {
    return new StdVector_cxx_0x5725934(StdVector_cxx_0x5725934.nativeCreateFactory0());
  }
}

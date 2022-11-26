package com.alibaba.graphscope.stdcxx;

import com.alibaba.graphscope.ds.GSVertexArray;
import java.lang.Double;

public class StdVector_cxx_0x8cbe72bfFactory implements StdVector.Factory<GSVertexArray<Double>> {
  public static final StdVector.Factory<GSVertexArray<Double>> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0x8cbe72bfFactory();
  }

  public StdVector_cxx_0x8cbe72bfFactory() {
  }

  public StdVector<GSVertexArray<Double>> create() {
    return new StdVector_cxx_0x8cbe72bf(StdVector_cxx_0x8cbe72bf.nativeCreateFactory0());
  }
}

package com.alibaba.graphscope.stdcxx;

import com.alibaba.graphscope.ds.GSVertexArray;
import java.lang.Integer;

public class StdVector_cxx_0x37a055Factory implements StdVector.Factory<GSVertexArray<Integer>> {
  public static final StdVector.Factory<GSVertexArray<Integer>> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0x37a055Factory();
  }

  public StdVector_cxx_0x37a055Factory() {
  }

  public StdVector<GSVertexArray<Integer>> create() {
    return new StdVector_cxx_0x37a055(StdVector_cxx_0x37a055.nativeCreateFactory0());
  }
}

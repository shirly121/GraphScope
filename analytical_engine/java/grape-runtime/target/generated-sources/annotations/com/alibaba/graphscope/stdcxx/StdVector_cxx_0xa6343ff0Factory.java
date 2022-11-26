package com.alibaba.graphscope.stdcxx;

import com.alibaba.graphscope.ds.Vertex;
import java.lang.Long;

public class StdVector_cxx_0xa6343ff0Factory implements StdVector.Factory<Vertex<Long>> {
  public static final StdVector.Factory<Vertex<Long>> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0xa6343ff0Factory();
  }

  public StdVector_cxx_0xa6343ff0Factory() {
  }

  public StdVector<Vertex<Long>> create() {
    return new StdVector_cxx_0xa6343ff0(StdVector_cxx_0xa6343ff0.nativeCreateFactory0());
  }
}

package com.alibaba.graphscope.stdcxx;

import java.lang.Double;

public class StdVector_cxx_0xcf11e077Factory implements StdVector.Factory<Double> {
  public static final StdVector.Factory<Double> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0xcf11e077Factory();
  }

  public StdVector_cxx_0xcf11e077Factory() {
  }

  public StdVector<Double> create() {
    return new StdVector_cxx_0xcf11e077(StdVector_cxx_0xcf11e077.nativeCreateFactory0());
  }
}

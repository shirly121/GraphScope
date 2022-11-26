package com.alibaba.graphscope.fragment.getter;

import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x34346601Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Double, Double> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Double, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x34346601Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x34346601Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Double, Double> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x34346601(ArrowProjectedFragmentGetter_cxx_0x34346601.nativeCreateFactory0());
  }
}

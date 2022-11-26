package com.alibaba.graphscope.fragment.getter;

import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x4fdb8a14Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Long, Double> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Long, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x4fdb8a14Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x4fdb8a14Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Long, Double> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x4fdb8a14(ArrowProjectedFragmentGetter_cxx_0x4fdb8a14.nativeCreateFactory0());
  }
}

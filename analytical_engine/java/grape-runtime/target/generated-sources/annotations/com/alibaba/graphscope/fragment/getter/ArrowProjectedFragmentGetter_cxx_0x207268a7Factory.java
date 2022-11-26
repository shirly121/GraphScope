package com.alibaba.graphscope.fragment.getter;

import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x207268a7Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Double, Integer> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Double, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x207268a7Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x207268a7Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Double, Integer> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x207268a7(ArrowProjectedFragmentGetter_cxx_0x207268a7.nativeCreateFactory0());
  }
}

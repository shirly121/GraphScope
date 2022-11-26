package com.alibaba.graphscope.fragment.getter;

import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x209d97e8Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Double, Long> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Double, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x209d97e8Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x209d97e8Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Double, Long> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x209d97e8(ArrowProjectedFragmentGetter_cxx_0x209d97e8.nativeCreateFactory0());
  }
}

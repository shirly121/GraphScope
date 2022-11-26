package com.alibaba.graphscope.fragment.getter;

import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0xefd3475Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, Double> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0xefd3475Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0xefd3475Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Integer, Double> create() {
    return new ArrowProjectedFragmentGetter_cxx_0xefd3475(ArrowProjectedFragmentGetter_cxx_0xefd3475.nativeCreateFactory0());
  }
}

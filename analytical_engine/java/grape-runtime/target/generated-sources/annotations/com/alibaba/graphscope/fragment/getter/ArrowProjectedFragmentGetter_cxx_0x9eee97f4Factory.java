package com.alibaba.graphscope.fragment.getter;

import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x9eee97f4Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, Long> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x9eee97f4Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x9eee97f4Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Integer, Long> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x9eee97f4(ArrowProjectedFragmentGetter_cxx_0x9eee97f4.nativeCreateFactory0());
  }
}

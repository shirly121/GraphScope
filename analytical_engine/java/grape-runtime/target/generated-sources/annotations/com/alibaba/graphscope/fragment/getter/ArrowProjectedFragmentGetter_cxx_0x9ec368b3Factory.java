package com.alibaba.graphscope.fragment.getter;

import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x9ec368b3Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, Integer> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x9ec368b3Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x9ec368b3Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Integer, Integer> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x9ec368b3(ArrowProjectedFragmentGetter_cxx_0x9ec368b3.nativeCreateFactory0());
  }
}

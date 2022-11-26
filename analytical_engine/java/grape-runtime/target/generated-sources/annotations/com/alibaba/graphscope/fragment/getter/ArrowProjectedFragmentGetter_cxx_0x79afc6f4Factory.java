package com.alibaba.graphscope.fragment.getter;

import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x79afc6f4Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Long, Integer> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Long, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x79afc6f4Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x79afc6f4Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Long, Integer> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x79afc6f4(ArrowProjectedFragmentGetter_cxx_0x79afc6f4.nativeCreateFactory0());
  }
}

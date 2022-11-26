package com.alibaba.graphscope.fragment.getter;

import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x79daf635Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Long, Long> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Long, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x79daf635Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x79daf635Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Long, Long> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x79daf635(ArrowProjectedFragmentGetter_cxx_0x79daf635.nativeCreateFactory0());
  }
}

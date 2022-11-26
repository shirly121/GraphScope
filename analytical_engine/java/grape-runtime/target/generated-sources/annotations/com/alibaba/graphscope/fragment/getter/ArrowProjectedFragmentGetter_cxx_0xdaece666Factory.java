package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0xdaece666Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, Double> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0xdaece666Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0xdaece666Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, StringView, Double> create() {
    return new ArrowProjectedFragmentGetter_cxx_0xdaece666(ArrowProjectedFragmentGetter_cxx_0xdaece666.nativeCreateFactory0());
  }
}

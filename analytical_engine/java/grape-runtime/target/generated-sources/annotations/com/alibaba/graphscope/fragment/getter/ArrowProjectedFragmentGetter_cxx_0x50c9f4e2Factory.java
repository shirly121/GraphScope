package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x50c9f4e2Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, Integer> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x50c9f4e2Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x50c9f4e2Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, StringView, Integer> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x50c9f4e2(ArrowProjectedFragmentGetter_cxx_0x50c9f4e2.nativeCreateFactory0());
  }
}

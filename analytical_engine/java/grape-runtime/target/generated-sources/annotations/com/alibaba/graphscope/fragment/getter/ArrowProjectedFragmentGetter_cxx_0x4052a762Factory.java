package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x4052a762Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, StringView> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Integer, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x4052a762Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x4052a762Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Integer, StringView> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x4052a762(ArrowProjectedFragmentGetter_cxx_0x4052a762.nativeCreateFactory0());
  }
}

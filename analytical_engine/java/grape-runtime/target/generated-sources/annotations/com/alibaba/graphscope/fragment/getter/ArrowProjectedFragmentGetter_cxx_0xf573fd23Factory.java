package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0xf573fd23Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Long, StringView> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Long, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0xf573fd23Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0xf573fd23Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Long, StringView> create() {
    return new ArrowProjectedFragmentGetter_cxx_0xf573fd23(ArrowProjectedFragmentGetter_cxx_0xf573fd23.nativeCreateFactory0());
  }
}

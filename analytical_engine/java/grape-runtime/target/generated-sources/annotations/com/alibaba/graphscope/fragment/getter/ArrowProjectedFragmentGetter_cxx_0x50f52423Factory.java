package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x50f52423Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, Long> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x50f52423Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x50f52423Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, StringView, Long> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x50f52423(ArrowProjectedFragmentGetter_cxx_0x50f52423.nativeCreateFactory0());
  }
}

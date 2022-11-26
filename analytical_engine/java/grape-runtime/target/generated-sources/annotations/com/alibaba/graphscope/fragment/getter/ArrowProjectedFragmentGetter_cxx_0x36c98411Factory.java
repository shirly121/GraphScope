package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x36c98411Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, StringView> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, StringView, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x36c98411Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x36c98411Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, StringView, StringView> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x36c98411(ArrowProjectedFragmentGetter_cxx_0x36c98411.nativeCreateFactory0());
  }
}

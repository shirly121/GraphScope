package com.alibaba.graphscope.fragment.getter;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentGetter_cxx_0x51d88d56Factory implements ArrowProjectedFragmentGetter.Factory<Long, Long, Double, StringView> {
  public static final ArrowProjectedFragmentGetter.Factory<Long, Long, Double, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentGetter_cxx_0x51d88d56Factory();
  }

  public ArrowProjectedFragmentGetter_cxx_0x51d88d56Factory() {
  }

  public ArrowProjectedFragmentGetter<Long, Long, Double, StringView> create() {
    return new ArrowProjectedFragmentGetter_cxx_0x51d88d56(ArrowProjectedFragmentGetter_cxx_0x51d88d56.nativeCreateFactory0());
  }
}

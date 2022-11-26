package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x6b3f75c7Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, StringView> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x6b3f75c7Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x6b3f75c7Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, StringView, StringView> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x6b3f75c7(ArrowProjectedFragmentMapper_cxx_0x6b3f75c7.nativeCreateFactory0());
  }
}

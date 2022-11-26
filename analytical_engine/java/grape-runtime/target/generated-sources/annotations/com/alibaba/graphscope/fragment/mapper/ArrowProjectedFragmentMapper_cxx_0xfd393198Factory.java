package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xfd393198Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, Integer> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xfd393198Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xfd393198Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, StringView, Integer> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xfd393198(ArrowProjectedFragmentMapper_cxx_0xfd393198.nativeCreateFactory0());
  }
}

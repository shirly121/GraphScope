package com.alibaba.graphscope.fragment.mapper;

import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xb1897069Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, Integer> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xb1897069Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xb1897069Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Integer, Integer> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xb1897069(ArrowProjectedFragmentMapper_cxx_0xb1897069.nativeCreateFactory0());
  }
}

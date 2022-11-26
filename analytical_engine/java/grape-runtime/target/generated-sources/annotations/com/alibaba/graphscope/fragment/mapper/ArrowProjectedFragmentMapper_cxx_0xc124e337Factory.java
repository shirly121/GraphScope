package com.alibaba.graphscope.fragment.mapper;

import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xc124e337Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Double, Double> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Double, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xc124e337Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xc124e337Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Double, Double> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xc124e337(ArrowProjectedFragmentMapper_cxx_0xc124e337.nativeCreateFactory0());
  }
}

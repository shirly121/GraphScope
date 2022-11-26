package com.alibaba.graphscope.fragment.mapper;

import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x60fab39eFactory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Long, Double> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Long, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x60fab39eFactory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x60fab39eFactory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Long, Double> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x60fab39e(ArrowProjectedFragmentMapper_cxx_0x60fab39e.nativeCreateFactory0());
  }
}

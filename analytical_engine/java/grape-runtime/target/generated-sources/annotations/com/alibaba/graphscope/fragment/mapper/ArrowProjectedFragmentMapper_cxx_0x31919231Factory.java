package com.alibaba.graphscope.fragment.mapper;

import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x31919231Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Double, Integer> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Double, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x31919231Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x31919231Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Double, Integer> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x31919231(ArrowProjectedFragmentMapper_cxx_0x31919231.nativeCreateFactory0());
  }
}

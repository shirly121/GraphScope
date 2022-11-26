package com.alibaba.graphscope.fragment.mapper;

import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x201c5dffFactory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, Double> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x201c5dffFactory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x201c5dffFactory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Integer, Double> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x201c5dff(ArrowProjectedFragmentMapper_cxx_0x201c5dff.nativeCreateFactory0());
  }
}

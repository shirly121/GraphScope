package com.alibaba.graphscope.fragment.mapper;

import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xb1b49faaFactory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, Long> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xb1b49faaFactory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xb1b49faaFactory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Integer, Long> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xb1b49faa(ArrowProjectedFragmentMapper_cxx_0xb1b49faa.nativeCreateFactory0());
  }
}

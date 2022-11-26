package com.alibaba.graphscope.fragment.mapper;

import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x8c75ceaaFactory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Long, Integer> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Long, Integer> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x8c75ceaaFactory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x8c75ceaaFactory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Long, Integer> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x8c75ceaa(ArrowProjectedFragmentMapper_cxx_0x8c75ceaa.nativeCreateFactory0());
  }
}

package com.alibaba.graphscope.fragment.mapper;

import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x8ca0fdebFactory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Long, Long> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Long, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x8ca0fdebFactory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x8ca0fdebFactory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Long, Long> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x8ca0fdeb(ArrowProjectedFragmentMapper_cxx_0x8ca0fdeb.nativeCreateFactory0());
  }
}

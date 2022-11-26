package com.alibaba.graphscope.fragment.mapper;

import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x31bcc172Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Double, Long> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Double, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x31bcc172Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x31bcc172Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Double, Long> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x31bcc172(ArrowProjectedFragmentMapper_cxx_0x31bcc172.nativeCreateFactory0());
  }
}

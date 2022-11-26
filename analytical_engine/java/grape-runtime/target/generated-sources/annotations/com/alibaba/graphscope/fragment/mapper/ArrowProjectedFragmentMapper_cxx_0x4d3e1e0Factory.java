package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x4d3e1e0Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Double, StringView> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Double, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x4d3e1e0Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x4d3e1e0Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Double, StringView> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x4d3e1e0(ArrowProjectedFragmentMapper_cxx_0x4d3e1e0.nativeCreateFactory0());
  }
}

package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Double;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0x8de83af0Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, Double> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, Double> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0x8de83af0Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0x8de83af0Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, StringView, Double> create() {
    return new ArrowProjectedFragmentMapper_cxx_0x8de83af0(ArrowProjectedFragmentMapper_cxx_0x8de83af0.nativeCreateFactory0());
  }
}

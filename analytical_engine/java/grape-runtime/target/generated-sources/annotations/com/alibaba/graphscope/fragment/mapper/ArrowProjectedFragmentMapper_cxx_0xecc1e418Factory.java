package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Integer;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xecc1e418Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, StringView> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Integer, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xecc1e418Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xecc1e418Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Integer, StringView> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xecc1e418(ArrowProjectedFragmentMapper_cxx_0xecc1e418.nativeCreateFactory0());
  }
}

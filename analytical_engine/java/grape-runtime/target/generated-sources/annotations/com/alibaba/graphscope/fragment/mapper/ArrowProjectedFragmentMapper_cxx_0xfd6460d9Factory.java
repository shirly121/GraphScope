package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xfd6460d9Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, Long> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, StringView, Long> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xfd6460d9Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xfd6460d9Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, StringView, Long> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xfd6460d9(ArrowProjectedFragmentMapper_cxx_0xfd6460d9.nativeCreateFactory0());
  }
}

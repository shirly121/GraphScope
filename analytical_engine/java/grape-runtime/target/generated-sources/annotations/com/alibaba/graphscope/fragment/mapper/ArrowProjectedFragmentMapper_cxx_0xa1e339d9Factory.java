package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.graphscope.ds.StringView;
import java.lang.Long;

public class ArrowProjectedFragmentMapper_cxx_0xa1e339d9Factory implements ArrowProjectedFragmentMapper.Factory<Long, Long, Long, StringView> {
  public static final ArrowProjectedFragmentMapper.Factory<Long, Long, Long, StringView> INSTANCE;

  static {
    INSTANCE = new ArrowProjectedFragmentMapper_cxx_0xa1e339d9Factory();
  }

  public ArrowProjectedFragmentMapper_cxx_0xa1e339d9Factory() {
  }

  public ArrowProjectedFragmentMapper<Long, Long, Long, StringView> create() {
    return new ArrowProjectedFragmentMapper_cxx_0xa1e339d9(ArrowProjectedFragmentMapper_cxx_0xa1e339d9.nativeCreateFactory0());
  }
}

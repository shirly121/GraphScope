package com.alibaba.graphscope.arrow.array;

import com.alibaba.graphscope.ds.StringView;

public class BaseArrowArrayBuilder_cxx_0x105010e2Factory implements BaseArrowArrayBuilder.Factory<StringView> {
  public static final BaseArrowArrayBuilder.Factory<StringView> INSTANCE;

  static {
    INSTANCE = new BaseArrowArrayBuilder_cxx_0x105010e2Factory();
  }

  public BaseArrowArrayBuilder_cxx_0x105010e2Factory() {
  }

  public BaseArrowArrayBuilder<StringView> create() {
    return new BaseArrowArrayBuilder_cxx_0x105010e2(BaseArrowArrayBuilder_cxx_0x105010e2.nativeCreateFactory0());
  }
}

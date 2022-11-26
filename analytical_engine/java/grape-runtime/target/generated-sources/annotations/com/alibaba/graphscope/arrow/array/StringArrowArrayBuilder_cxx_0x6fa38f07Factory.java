package com.alibaba.graphscope.arrow.array;

public class StringArrowArrayBuilder_cxx_0x6fa38f07Factory implements StringArrowArrayBuilder.Factory {
  public static final StringArrowArrayBuilder.Factory INSTANCE;

  static {
    INSTANCE = new StringArrowArrayBuilder_cxx_0x6fa38f07Factory();
  }

  public StringArrowArrayBuilder_cxx_0x6fa38f07Factory() {
  }

  public StringArrowArrayBuilder create() {
    return new StringArrowArrayBuilder_cxx_0x6fa38f07(StringArrowArrayBuilder_cxx_0x6fa38f07.nativeCreateFactory0());
  }
}

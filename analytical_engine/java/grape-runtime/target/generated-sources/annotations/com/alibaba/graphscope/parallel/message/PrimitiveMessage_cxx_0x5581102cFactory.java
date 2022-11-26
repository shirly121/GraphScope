package com.alibaba.graphscope.parallel.message;

import java.lang.Long;

public class PrimitiveMessage_cxx_0x5581102cFactory implements PrimitiveMessage.Factory<Long> {
  public static final PrimitiveMessage.Factory<Long> INSTANCE;

  static {
    INSTANCE = new PrimitiveMessage_cxx_0x5581102cFactory();
  }

  public PrimitiveMessage_cxx_0x5581102cFactory() {
  }

  public PrimitiveMessage<Long> create() {
    return new PrimitiveMessage_cxx_0x5581102c(PrimitiveMessage_cxx_0x5581102c.nativeCreateFactory0());
  }

  public PrimitiveMessage<Long> create(Long arg0) {
    return new PrimitiveMessage_cxx_0x5581102c(PrimitiveMessage_cxx_0x5581102c.nativeCreateFactory1(arg0));
  }
}

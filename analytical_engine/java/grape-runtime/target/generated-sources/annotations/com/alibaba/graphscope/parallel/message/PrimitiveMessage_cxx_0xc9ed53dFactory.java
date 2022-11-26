package com.alibaba.graphscope.parallel.message;

import java.lang.Double;

public class PrimitiveMessage_cxx_0xc9ed53dFactory implements PrimitiveMessage.Factory<Double> {
  public static final PrimitiveMessage.Factory<Double> INSTANCE;

  static {
    INSTANCE = new PrimitiveMessage_cxx_0xc9ed53dFactory();
  }

  public PrimitiveMessage_cxx_0xc9ed53dFactory() {
  }

  public PrimitiveMessage<Double> create() {
    return new PrimitiveMessage_cxx_0xc9ed53d(PrimitiveMessage_cxx_0xc9ed53d.nativeCreateFactory0());
  }

  public PrimitiveMessage<Double> create(Double arg0) {
    return new PrimitiveMessage_cxx_0xc9ed53d(PrimitiveMessage_cxx_0xc9ed53d.nativeCreateFactory1(arg0));
  }
}

package com.alibaba.graphscope.ds;

import java.lang.Integer;

public class Vertex_cxx_0xaca404e3Factory implements Vertex.Factory<Integer> {
  public static final Vertex.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new Vertex_cxx_0xaca404e3Factory();
  }

  public Vertex_cxx_0xaca404e3Factory() {
  }

  public Vertex<Integer> create() {
    return new Vertex_cxx_0xaca404e3(Vertex_cxx_0xaca404e3.nativeCreateFactory0());
  }
}

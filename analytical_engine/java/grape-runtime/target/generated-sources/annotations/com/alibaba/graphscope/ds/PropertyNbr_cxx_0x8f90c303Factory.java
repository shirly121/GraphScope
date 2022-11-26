package com.alibaba.graphscope.ds;

import java.lang.Integer;

public class PropertyNbr_cxx_0x8f90c303Factory implements PropertyNbr.Factory<Integer> {
  public static final PropertyNbr.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new PropertyNbr_cxx_0x8f90c303Factory();
  }

  public PropertyNbr_cxx_0x8f90c303Factory() {
  }

  public PropertyNbr<Integer> create() {
    return new PropertyNbr_cxx_0x8f90c303(PropertyNbr_cxx_0x8f90c303.nativeCreateFactory0());
  }
}

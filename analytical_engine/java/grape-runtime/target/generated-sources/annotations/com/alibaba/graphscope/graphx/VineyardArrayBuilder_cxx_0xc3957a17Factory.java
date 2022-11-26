package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.stdcxx.StdVector;
import java.lang.Double;

public class VineyardArrayBuilder_cxx_0xc3957a17Factory implements VineyardArrayBuilder.Factory<Double> {
  public static final VineyardArrayBuilder.Factory<Double> INSTANCE;

  static {
    INSTANCE = new VineyardArrayBuilder_cxx_0xc3957a17Factory();
  }

  public VineyardArrayBuilder_cxx_0xc3957a17Factory() {
  }

  public VineyardArrayBuilder<Double> create(VineyardClient arg0, StdVector<Double> arg1) {
    return new VineyardArrayBuilder_cxx_0xc3957a17(VineyardArrayBuilder_cxx_0xc3957a17.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address));
  }

  public VineyardArrayBuilder<Double> create(VineyardClient arg0, long arg1) {
    return new VineyardArrayBuilder_cxx_0xc3957a17(VineyardArrayBuilder_cxx_0xc3957a17.nativeCreateFactory1(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

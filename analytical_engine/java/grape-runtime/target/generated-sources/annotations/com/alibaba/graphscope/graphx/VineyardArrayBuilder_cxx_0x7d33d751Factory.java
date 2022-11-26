package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.stdcxx.StdVector;
import java.lang.Integer;

public class VineyardArrayBuilder_cxx_0x7d33d751Factory implements VineyardArrayBuilder.Factory<Integer> {
  public static final VineyardArrayBuilder.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new VineyardArrayBuilder_cxx_0x7d33d751Factory();
  }

  public VineyardArrayBuilder_cxx_0x7d33d751Factory() {
  }

  public VineyardArrayBuilder<Integer> create(VineyardClient arg0, StdVector<Integer> arg1) {
    return new VineyardArrayBuilder_cxx_0x7d33d751(VineyardArrayBuilder_cxx_0x7d33d751.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address));
  }

  public VineyardArrayBuilder<Integer> create(VineyardClient arg0, long arg1) {
    return new VineyardArrayBuilder_cxx_0x7d33d751(VineyardArrayBuilder_cxx_0x7d33d751.nativeCreateFactory1(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

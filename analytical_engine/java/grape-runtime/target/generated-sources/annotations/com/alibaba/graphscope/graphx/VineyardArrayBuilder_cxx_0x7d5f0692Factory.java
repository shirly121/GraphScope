package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.stdcxx.StdVector;
import java.lang.Long;

public class VineyardArrayBuilder_cxx_0x7d5f0692Factory implements VineyardArrayBuilder.Factory<Long> {
  public static final VineyardArrayBuilder.Factory<Long> INSTANCE;

  static {
    INSTANCE = new VineyardArrayBuilder_cxx_0x7d5f0692Factory();
  }

  public VineyardArrayBuilder_cxx_0x7d5f0692Factory() {
  }

  public VineyardArrayBuilder<Long> create(VineyardClient arg0, StdVector<Long> arg1) {
    return new VineyardArrayBuilder_cxx_0x7d5f0692(VineyardArrayBuilder_cxx_0x7d5f0692.nativeCreateFactory0(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address));
  }

  public VineyardArrayBuilder<Long> create(VineyardClient arg0, long arg1) {
    return new VineyardArrayBuilder_cxx_0x7d5f0692(VineyardArrayBuilder_cxx_0x7d5f0692.nativeCreateFactory1(((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1));
  }
}

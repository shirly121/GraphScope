package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,double,double>>")
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdSharedPtr")
public class StdSharedPtr_cxx_0x8731cca3 extends FFIPointerImpl implements StdSharedPtr<ArrowProjectedFragment<Long, Long, Double, Double>> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StdSharedPtr_cxx_0x8731cca3.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StdSharedPtr_cxx_0x8731cca3(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StdSharedPtr_cxx_0x8731cca3 that = (StdSharedPtr_cxx_0x8731cca3) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public ArrowProjectedFragment<Long, Long, Double, Double> get() {
    long ret$ = nativeGet(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.fragment.ArrowProjectedFragment_cxx_0xe3684916(ret$));
  }

  public static native long nativeGet(long ptr);
}

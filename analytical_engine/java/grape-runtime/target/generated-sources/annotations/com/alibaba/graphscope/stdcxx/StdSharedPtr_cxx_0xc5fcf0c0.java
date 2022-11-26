package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.column.DoubleColumn;
import com.alibaba.graphscope.fragment.ArrowFragment;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("std::shared_ptr<gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>>")
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdSharedPtr")
public class StdSharedPtr_cxx_0xc5fcf0c0 extends FFIPointerImpl implements StdSharedPtr<DoubleColumn<ArrowFragment<Long>>> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StdSharedPtr_cxx_0xc5fcf0c0.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StdSharedPtr_cxx_0xc5fcf0c0(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StdSharedPtr_cxx_0xc5fcf0c0 that = (StdSharedPtr_cxx_0xc5fcf0c0) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public DoubleColumn<ArrowFragment<Long>> get() {
    long ret$ = nativeGet(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.column.DoubleColumn_cxx_0x26811983(ret$));
  }

  public static native long nativeGet(long ptr);
}

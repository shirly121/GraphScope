package com.alibaba.graphscope.arrow.array;

import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.StringView;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::ArrowArray<std::string>")
@FFISynthetic("com.alibaba.graphscope.arrow.array.StringArrowArray")
public class StringArrowArray_cxx_0x58f735ea extends BaseArrowArray_cxx_0x58f735ea implements StringArrowArray {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StringArrowArray_cxx_0x58f735ea.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StringArrowArray_cxx_0x58f735ea(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringArrowArray_cxx_0x58f735ea that = (StringArrowArray_cxx_0x58f735ea) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("GetString")
  @CXXValue
  public FFIByteString getString(long index) {
    long ret$ = nativeGetString(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274.SIZE), index); return (new com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274(ret$));
  }

  @FFINameAlias("GetString")
  @CXXValue
  public static native long nativeGetString(long ptr, long rv_base, long index0);

  @FFINameAlias("GetView")
  @CXXValue
  public StringView getView(long index) {
    long ret$ = nativeGetView(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.StringView_cxx_0x58a38028.SIZE), index); return (new com.alibaba.graphscope.ds.StringView_cxx_0x58a38028(ret$));
  }

  @FFINameAlias("GetView")
  @CXXValue
  public static native long nativeGetView(long ptr, long rv_base, long index0);
}

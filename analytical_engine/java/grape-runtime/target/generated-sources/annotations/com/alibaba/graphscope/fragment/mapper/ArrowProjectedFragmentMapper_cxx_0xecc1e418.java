package com.alibaba.graphscope.fragment.mapper;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.arrow.array.BaseArrowArrayBuilder;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::ArrowProjectedFragmentMapper<int64_t,uint64_t,int32_t,std::string>",
    factory = ArrowProjectedFragmentMapper_cxx_0xecc1e418Factory.class
)
@FFISynthetic("com.alibaba.graphscope.fragment.mapper.ArrowProjectedFragmentMapper")
public class ArrowProjectedFragmentMapper_cxx_0xecc1e418 extends FFIPointerImpl implements ArrowProjectedFragmentMapper<Long, Long, Integer, StringView> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ArrowProjectedFragmentMapper_cxx_0xecc1e418.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ArrowProjectedFragmentMapper_cxx_0xecc1e418(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowProjectedFragmentMapper_cxx_0xecc1e418 that = (ArrowProjectedFragmentMapper_cxx_0xecc1e418) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("Map")
  @CXXValue
  public StdSharedPtr<ArrowProjectedFragment<Long, Long, Integer, StringView>> map(
      @CXXReference StdSharedPtr<ArrowFragment<Long>> arg0, int arg1, int arg2,
      @CXXReference BaseArrowArrayBuilder<Integer> arg3,
      @CXXReference BaseArrowArrayBuilder<StringView> arg4, @CXXReference VineyardClient arg5) {
    long ret$ = nativeMap0(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0x35726ae2.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1, arg2, ((com.alibaba.fastffi.FFIPointerImpl) arg3).address, ((com.alibaba.fastffi.FFIPointerImpl) arg4).address, ((com.alibaba.fastffi.FFIPointerImpl) arg5).address); return (new com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0x35726ae2(ret$));
  }

  @FFINameAlias("Map")
  @CXXValue
  public static native long nativeMap0(long ptr, long rv_base, long arg00, int arg11, int arg22,
      long arg33, long arg44, long arg55);

  @FFINameAlias("Map")
  @CXXValue
  public StdSharedPtr<ArrowProjectedFragment<Long, Long, Integer, StringView>> map(
      @CXXReference StdSharedPtr<ArrowFragment<Long>> arg0, int arg1, int arg2,
      @CXXReference BaseArrowArrayBuilder<Integer> arg3, @CXXReference VineyardClient arg4) {
    long ret$ = nativeMap1(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0x35726ae2.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1, arg2, ((com.alibaba.fastffi.FFIPointerImpl) arg3).address, ((com.alibaba.fastffi.FFIPointerImpl) arg4).address); return (new com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0x35726ae2(ret$));
  }

  @FFINameAlias("Map")
  @CXXValue
  public static native long nativeMap1(long ptr, long rv_base, long arg00, int arg11, int arg22,
      long arg33, long arg44);

  public static native long nativeCreateFactory0();
}

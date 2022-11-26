package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.runtime.UnusedImpl;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.lang.Double;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;

@FFIForeignType(
    value = "grape::MessageInBuffer",
    factory = MessageInBufferGen_cxx_0x2b9fb1a1Factory.class
)
@FFISynthetic("com.alibaba.graphscope.parallel.MessageInBufferGen")
public class MessageInBufferGen_cxx_0x2b9fb1a1 extends FFIPointerImpl implements MessageInBufferGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public MessageInBufferGen_cxx_0x2b9fb1a1(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MessageInBufferGen_cxx_0x2b9fb1a1 that = (MessageInBufferGen_cxx_0x2b9fb1a1) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
          "grape::EmptyType"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
          "com.alibaba.graphscope.ds.EmptyType"
      }
  )
  public boolean getMessageArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("grape::EmptyType") @CXXReference EmptyType arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongEmpty arg3) {
    return nativeGetMessageArrowProjected0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
          "grape::EmptyType"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
          "com.alibaba.graphscope.ds.EmptyType"
      }
  )
  public static native boolean nativeGetMessageArrowProjected0(long ptr, long arg00, long arg11,
      long arg22);

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
          "gs::DoubleMsg"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
          "com.alibaba.graphscope.parallel.message.DoubleMsg"
      }
  )
  public boolean getMessageArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleIntegerDouble arg3) {
    return nativeGetMessageArrowProjected1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
          "gs::DoubleMsg"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
          "com.alibaba.graphscope.parallel.message.DoubleMsg"
      }
  )
  public static native boolean nativeGetMessageArrowProjected1(long ptr, long arg00, long arg11,
      long arg22);

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
          "gs::LongMsg"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>",
          "com.alibaba.graphscope.parallel.message.LongMsg"
      }
  )
  public boolean getMessageArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::LongMsg") @CXXReference LongMsg arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongLong arg3) {
    return nativeGetMessageArrowProjected2(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
          "gs::LongMsg"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>",
          "com.alibaba.graphscope.parallel.message.LongMsg"
      }
  )
  public static native boolean nativeGetMessageArrowProjected2(long ptr, long arg00, long arg11,
      long arg22);

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public boolean getPureMessage(
      @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg0) {
    return nativeGetPureMessage0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public static native boolean nativeGetPureMessage0(long ptr, long arg00);

  public static native long nativeCreateFactory0();
}

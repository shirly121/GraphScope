package com.alibaba.graphscope.communication;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.lang.Object;
import java.lang.String;

@FFIForeignType("grape::Communicator")
@FFISynthetic("com.alibaba.graphscope.communication.FFICommunicatorGen")
public class FFICommunicatorGen_cxx_0xd6e741c0 extends FFIPointerImpl implements FFICommunicatorGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public FFICommunicatorGen_cxx_0xd6e741c0(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FFICommunicatorGen_cxx_0xd6e741c0 that = (FFICommunicatorGen_cxx_0xd6e741c0) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("RecvFrom")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public void receiveFrom(int arg0,
      @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg1) {
    nativeReceiveFrom0(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("RecvFrom")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public static native void nativeReceiveFrom0(long ptr, int arg00, long arg11);
}

package com.alibaba.graphscope.parallel;

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

@FFIForeignType("grape::DefaultMessageManager")
@FFISynthetic("com.alibaba.graphscope.parallel.DefaultMessageManagerGen")
public class DefaultMessageManagerGen_cxx_0xb936493c extends FFIPointerImpl implements DefaultMessageManagerGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public DefaultMessageManagerGen_cxx_0xb936493c(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DefaultMessageManagerGen_cxx_0xb936493c that = (DefaultMessageManagerGen_cxx_0xb936493c) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("FinishARound")
  public void finishARound() {
    nativeFinishARound(address);
  }

  @FFINameAlias("FinishARound")
  public static native void nativeFinishARound(long ptr);

  @FFINameAlias("ForceContinue")
  public void forceContinue() {
    nativeForceContinue(address);
  }

  @FFINameAlias("ForceContinue")
  public static native void nativeForceContinue(long ptr);

  @FFINameAlias("GetMsgSize")
  public long getMsgSize() {
    return nativeGetMsgSize(address);
  }

  @FFINameAlias("GetMsgSize")
  public static native long nativeGetMsgSize(long ptr);

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

  @FFINameAlias("SendToFragment")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public void sendToFragment(int arg0,
      @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg1) {
    nativeSendToFragment0(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("SendToFragment")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public static native void nativeSendToFragment0(long ptr, int arg00, long arg11);

  @FFINameAlias("Start")
  public void start() {
    nativeStart(address);
  }

  @FFINameAlias("Start")
  public static native void nativeStart(long ptr);

  @FFINameAlias("StartARound")
  public void startARound() {
    nativeStartARound(address);
  }

  @FFINameAlias("StartARound")
  public static native void nativeStartARound(long ptr);

  @FFINameAlias("ToTerminate")
  public boolean toTerminate() {
    return nativeToTerminate(address);
  }

  @FFINameAlias("ToTerminate")
  public static native boolean nativeToTerminate(long ptr);
}

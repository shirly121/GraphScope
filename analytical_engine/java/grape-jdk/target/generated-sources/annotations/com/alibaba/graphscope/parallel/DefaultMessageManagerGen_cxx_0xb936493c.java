package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("grape::DefaultMessageManager")
@FFISynthetic("com.alibaba.graphscope.parallel.DefaultMessageManagerGen")
public class DefaultMessageManagerGen_cxx_0xb936493c extends FFIPointerImpl implements DefaultMessageManagerGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(DefaultMessageManagerGen_cxx_0xb936493c.class, "grape-jni"));
    }
  }
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

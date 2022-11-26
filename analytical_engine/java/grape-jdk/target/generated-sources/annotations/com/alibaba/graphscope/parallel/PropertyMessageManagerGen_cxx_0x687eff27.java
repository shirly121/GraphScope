package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::PropertyMessageManager")
@FFISynthetic("com.alibaba.graphscope.parallel.PropertyMessageManagerGen")
public class PropertyMessageManagerGen_cxx_0x687eff27 extends FFIPointerImpl implements PropertyMessageManagerGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PropertyMessageManagerGen_cxx_0x687eff27.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PropertyMessageManagerGen_cxx_0x687eff27(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PropertyMessageManagerGen_cxx_0x687eff27 that = (PropertyMessageManagerGen_cxx_0x687eff27) o;
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

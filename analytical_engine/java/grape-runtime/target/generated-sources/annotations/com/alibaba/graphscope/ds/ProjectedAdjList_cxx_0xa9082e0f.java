package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,std::string>")
@FFISynthetic("com.alibaba.graphscope.ds.ProjectedAdjList")
public class ProjectedAdjList_cxx_0xa9082e0f extends FFIPointerImpl implements ProjectedAdjList<Long, StringView> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ProjectedAdjList_cxx_0xa9082e0f.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ProjectedAdjList_cxx_0xa9082e0f(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProjectedAdjList_cxx_0xa9082e0f that = (ProjectedAdjList_cxx_0xa9082e0f) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXValue
  public ProjectedNbr<Long, StringView> begin() {
    long ret$ = nativeBegin(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.ProjectedNbr_cxx_0x8900456.SIZE)); return (new com.alibaba.graphscope.ds.ProjectedNbr_cxx_0x8900456(ret$));
  }

  @CXXValue
  public static native long nativeBegin(long ptr, long rv_base);

  @FFINameAlias("Empty")
  public boolean empty() {
    return nativeEmpty(address);
  }

  @FFINameAlias("Empty")
  public static native boolean nativeEmpty(long ptr);

  @CXXValue
  public ProjectedNbr<Long, StringView> end() {
    long ret$ = nativeEnd(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.ProjectedNbr_cxx_0x8900456.SIZE)); return (new com.alibaba.graphscope.ds.ProjectedNbr_cxx_0x8900456(ret$));
  }

  @CXXValue
  public static native long nativeEnd(long ptr, long rv_base);

  @FFINameAlias("NotEmpty")
  public boolean notEmpty() {
    return nativeNotEmpty(address);
  }

  @FFINameAlias("NotEmpty")
  public static native boolean nativeNotEmpty(long ptr);

  @FFINameAlias("Size")
  public long size() {
    return nativeSize(address);
  }

  @FFINameAlias("Size")
  public static native long nativeSize(long ptr);
}

package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.runtime.UnusedImpl;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;

@FFIForeignType("grape::ParallelMessageManager")
@FFISynthetic("com.alibaba.graphscope.parallel.ParallelMessageManagerGen")
public class ParallelMessageManagerGen_cxx_0x5a835738 extends FFIPointerImpl implements ParallelMessageManagerGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ParallelMessageManagerGen_cxx_0x5a835738(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParallelMessageManagerGen_cxx_0x5a835738 that = (ParallelMessageManagerGen_cxx_0x5a835738) o;
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

  @FFINameAlias("GetMessageInBuffer")
  public boolean getMessageInBuffer(@CXXReference MessageInBuffer arg0) {
    return nativeGetMessageInBuffer(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetMessageInBuffer")
  public static native boolean nativeGetMessageInBuffer(long ptr, long arg00);

  @FFINameAlias("GetMsgSize")
  public long getMsgSize() {
    return nativeGetMsgSize(address);
  }

  @FFINameAlias("GetMsgSize")
  public static native long nativeGetMsgSize(long ptr);

  @FFINameAlias("InitChannels")
  public void initChannels(int arg0) {
    nativeInitChannels(address, arg0);
  }

  @FFINameAlias("InitChannels")
  public static native void nativeInitChannels(long ptr, int arg00);

  @FFINameAlias("SendMsgThroughIEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>",
          "double"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Integer>",
          "java.lang.Double"
      }
  )
  public void sendMsgThroughIEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Integer> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("double") @CXXReference Double arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleIntegerDouble arg4) {
    nativeSendMsgThroughIEdgesArrowProjected0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, arg2, arg3);
  }

  @FFINameAlias("SendMsgThroughIEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>",
          "double"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Integer>",
          "java.lang.Double"
      }
  )
  public static native void nativeSendMsgThroughIEdgesArrowProjected0(long ptr, long arg00,
      long arg11, double arg22, int arg33);

  @FFINameAlias("SendMsgThroughIEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
          "double"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
          "java.lang.Double"
      }
  )
  public void sendMsgThroughIEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("double") @CXXReference Double arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongDouble arg4) {
    nativeSendMsgThroughIEdgesArrowProjected1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, arg2, arg3);
  }

  @FFINameAlias("SendMsgThroughIEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
          "double"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
          "java.lang.Double"
      }
  )
  public static native void nativeSendMsgThroughIEdgesArrowProjected1(long ptr, long arg00,
      long arg11, double arg22, int arg33);

  @FFINameAlias("SendMsgThroughIEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
          "double"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>",
          "java.lang.Double"
      }
  )
  public void sendMsgThroughIEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("double") @CXXReference Double arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongDouble arg4) {
    nativeSendMsgThroughIEdgesArrowProjected2(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, arg2, arg3);
  }

  @FFINameAlias("SendMsgThroughIEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
          "double"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>",
          "java.lang.Double"
      }
  )
  public static native void nativeSendMsgThroughIEdgesArrowProjected2(long ptr, long arg00,
      long arg11, double arg22, int arg33);

  @FFINameAlias("SendMsgThroughOEdges")
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
  public void sendMsgThroughOEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongDouble arg4) {
    nativeSendMsgThroughOEdgesArrowProjected0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address, arg3);
  }

  @FFINameAlias("SendMsgThroughOEdges")
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
  public static native void nativeSendMsgThroughOEdgesArrowProjected0(long ptr, long arg00,
      long arg11, long arg22, int arg33);

  @FFINameAlias("SendMsgThroughOEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
          "gs::DoubleMsg"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>",
          "com.alibaba.graphscope.parallel.message.DoubleMsg"
      }
  )
  public void sendMsgThroughOEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongDouble arg4) {
    nativeSendMsgThroughOEdgesArrowProjected1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address, arg3);
  }

  @FFINameAlias("SendMsgThroughOEdges")
  @CXXTemplate(
      cxx = {
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
          "gs::DoubleMsg"
      },
      java = {
          "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>",
          "com.alibaba.graphscope.parallel.message.DoubleMsg"
      }
  )
  public static native void nativeSendMsgThroughOEdgesArrowProjected1(long ptr, long arg00,
      long arg11, long arg22, int arg33);

  @FFINameAlias("SendToFragment")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public void sendToFragment(int arg0,
      @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg1, int arg2) {
    nativeSendToFragment0(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, arg2);
  }

  @FFINameAlias("SendToFragment")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  public static native void nativeSendToFragment0(long ptr, int arg00, long arg11, int arg22);

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

  @FFINameAlias("SyncStateOnOuterVertex")
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
  public void syncStateOnOuterVertexArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongDouble arg4) {
    nativeSyncStateOnOuterVertexArrowProjected0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address, arg3);
  }

  @FFINameAlias("SyncStateOnOuterVertex")
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
  public static native void nativeSyncStateOnOuterVertexArrowProjected0(long ptr, long arg00,
      long arg11, long arg22, int arg33);

  @FFINameAlias("SyncStateOnOuterVertex")
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
  public void syncStateOnOuterVertexArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::LongMsg") @CXXReference LongMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongLong arg4) {
    nativeSyncStateOnOuterVertexArrowProjected1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address, arg3);
  }

  @FFINameAlias("SyncStateOnOuterVertex")
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
  public static native void nativeSyncStateOnOuterVertexArrowProjected1(long ptr, long arg00,
      long arg11, long arg22, int arg33);

  @FFINameAlias("SyncStateOnOuterVertex")
  @CXXTemplate(
      cxx = "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
      java = "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>"
  )
  public void syncStateOnOuterVertexArrowProjectedNoMsg(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1, int arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLong arg3) {
    nativeSyncStateOnOuterVertexArrowProjectedNoMsg0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, arg2);
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  @CXXTemplate(
      cxx = "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
      java = "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>"
  )
  public static native void nativeSyncStateOnOuterVertexArrowProjectedNoMsg0(long ptr, long arg00,
      long arg11, int arg22);

  @FFINameAlias("SyncStateOnOuterVertex")
  @CXXTemplate(
      cxx = "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
      java = "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>"
  )
  public void syncStateOnOuterVertexArrowProjectedNoMsg(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1, int arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLong arg3) {
    nativeSyncStateOnOuterVertexArrowProjectedNoMsg1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address, arg2);
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  @CXXTemplate(
      cxx = "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
      java = "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>"
  )
  public static native void nativeSyncStateOnOuterVertexArrowProjectedNoMsg1(long ptr, long arg00,
      long arg11, int arg22);

  @FFINameAlias("ToTerminate")
  public boolean toTerminate() {
    return nativeToTerminate(address);
  }

  @FFINameAlias("ToTerminate")
  public static native boolean nativeToTerminate(long ptr);
}

package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFunGen;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.runtime.UnusedImpl;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;

@FFITypeAlias("grape::ParallelMessageManager")
@CXXHead({
    "grape/parallel/parallel_message_manager.h",
    "core/fragment/arrow_projected_fragment.h",
    "grape/fragment/immutable_edgecut_fragment.h",
    "grape/graph/adj_list.h",
    "core/java/java_messages.h"
})
@FFIGen(
    type = "com.alibaba.graphscope.parallel.ParallelMessageManager",
    functionTemplates = {
        @FFIFunGen(name = "sendToFragment", returnType = "void", parameterTypes = {"int", "MSG_T", "int"}, templates = @CXXTemplate(java = "com.alibaba.graphscope.stdcxx.FFIByteVector", cxx = "std::vector<char>")),
        @FFIFunGen(name = "sendMsgThroughIEdgesArrowProjected", returnType = "void", parameterTypes = {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"}, templates = {@CXXTemplate(java = {"Long", "Long", "Double", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "java.lang.Double", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"}, cxx = {"int64_t", "uint64_t", "double", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>", "double", "any"}), @CXXTemplate(java = {"Long", "Long", "Long", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>", "java.lang.Double", "com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble"}, cxx = {"int64_t", "uint64_t", "int64_t", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>", "double", "any"}), @CXXTemplate(java = {"Long", "Long", "Double", "Integer", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Integer>", "java.lang.Double", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleIntegerDouble"}, cxx = {"int64_t", "uint64_t", "double", "int32_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>", "double", "any"})}),
        @FFIFunGen(name = "sendMsgThroughOEdgesArrowProjected", returnType = "void", parameterTypes = {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"}, templates = {@CXXTemplate(java = {"Long", "Long", "Long", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>", "com.alibaba.graphscope.parallel.message.DoubleMsg", "com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble"}, cxx = {"int64_t", "uint64_t", "int64_t", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>", "gs::DoubleMsg", "any"}), @CXXTemplate(java = {"Long", "Long", "Double", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "com.alibaba.graphscope.parallel.message.DoubleMsg", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"}, cxx = {"int64_t", "uint64_t", "double", "Long", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>", "gs::DoubleMsg", "any"})}),
        @FFIFunGen(name = "syncStateOnOuterVertexArrowProjected", returnType = "void", parameterTypes = {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"}, templates = {@CXXTemplate(java = {"Long", "Long", "Double", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "com.alibaba.graphscope.parallel.message.DoubleMsg", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"}, cxx = {"int64_t", "uint64_t", "double", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>", "gs::DoubleMsg", "any"}), @CXXTemplate(java = {"Long", "Long", "Long", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>", "com.alibaba.graphscope.parallel.message.LongMsg", "com.alibaba.graphscope.runtime.UnusedImpl.LongLongLong"}, cxx = {"int64_t", "uint64_t", "int64_t", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>", "gs::LongMsg", "any"})}),
        @FFIFunGen(name = "syncStateOnOuterVertexArrowProjectedNoMsg", returnType = "void", parameterTypes = {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "int", "UNUSED_T"}, templates = {@CXXTemplate(java = {"Long", "Long", "Double", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLong"}, cxx = {"int64_t", "uint64_t", "double", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>", "any"}), @CXXTemplate(java = {"Long", "Long", "Long", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>", "com.alibaba.graphscope.runtime.UnusedImpl.LongLong"}, cxx = {"int64_t", "uint64_t", "int64_t", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>", "any"})})
    }
)
@FFISynthetic("com.alibaba.graphscope.parallel.ParallelMessageManager")
public abstract interface ParallelMessageManagerGen extends ParallelMessageManager {
  @FFINameAlias("SendToFragment")
  default <MSG_T> void sendToFragment(int arg0, @CXXReference MSG_T arg1, int arg2) {
    if (arg1 == null) {
      throw new NullPointerException("arg1 must not be null");
    }
    if (arg1 instanceof FFIByteVector)  {
      sendToFragment(arg0, (com.alibaba.graphscope.stdcxx.FFIByteVector) arg1, arg2); return;
    }
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendToFragment, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendToFragment")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  void sendToFragment(int arg0, @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg1,
      int arg2);

  @FFINameAlias("SyncStateOnOuterVertex")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void syncStateOnOuterVertexImmutable(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void syncStateOnOuterVertexArrowProjected(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    if (arg2 == null) {
      throw new NullPointerException("arg2 must not be null");
    }
    if (arg2 instanceof DoubleMsg)  {
      syncStateOnOuterVertexArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.parallel.message.DoubleMsg) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble) arg4); return;
    }
    if (arg2 instanceof LongMsg)  {
      syncStateOnOuterVertexArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.parallel.message.LongMsg) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.LongLongLong) arg4); return;
    }
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexArrowProjected, no template instantiation for the type arguments.");
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
  void syncStateOnOuterVertexArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongDouble arg4);

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
  void syncStateOnOuterVertexArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::LongMsg") @CXXReference LongMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongLong arg4);

  @FFINameAlias("SyncStateOnOuterVertex")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>, UNUSED_T> void syncStateOnOuterVertexImmutableNoMsg(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, int arg2,
      @FFISkip UNUSED_T arg3) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexImmutableNoMsg, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>, UNUSED_T> void syncStateOnOuterVertexArrowProjectedNoMsg(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, int arg2,
      @FFISkip UNUSED_T arg3) {
    if (arg3 == null) {
      throw new NullPointerException("arg3 must not be null");
    }
    if (arg3 instanceof UnusedImpl.DoubleLong)  {
      syncStateOnOuterVertexArrowProjectedNoMsg((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, arg2, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleLong) arg3); return;
    }
    if (arg3 instanceof UnusedImpl.LongLong)  {
      syncStateOnOuterVertexArrowProjectedNoMsg((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, arg2, (com.alibaba.graphscope.runtime.UnusedImpl.LongLong) arg3); return;
    }
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexArrowProjectedNoMsg, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  @CXXTemplate(
      cxx = "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>",
      java = "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>"
  )
  void syncStateOnOuterVertexArrowProjectedNoMsg(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1, int arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLong arg3);

  @FFINameAlias("SyncStateOnOuterVertex")
  @CXXTemplate(
      cxx = "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>",
      java = "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>"
  )
  void syncStateOnOuterVertexArrowProjectedNoMsg(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1, int arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLong arg3);

  @FFINameAlias("SendMsgThroughOEdges")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void sendMsgThroughOEdgesImmutable(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughOEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void sendMsgThroughOEdgesArrowProjected(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    if (arg4 == null) {
      throw new NullPointerException("arg4 must not be null");
    }
    if (arg4 instanceof UnusedImpl.LongLongDouble)  {
      sendMsgThroughOEdgesArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.parallel.message.DoubleMsg) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble) arg4); return;
    }
    if (arg4 instanceof UnusedImpl.DoubleLongDouble)  {
      sendMsgThroughOEdgesArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.parallel.message.DoubleMsg) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble) arg4); return;
    }
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughOEdgesArrowProjected, no template instantiation for the type arguments.");
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
  void sendMsgThroughOEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongDouble arg4);

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
  void sendMsgThroughOEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongDouble arg4);

  @FFINameAlias("SendMsgThroughEdges")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void sendMsgThroughEdgesImmutable(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void sendMsgThroughEdgesArrowProjected(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void sendMsgThroughIEdgesImmutable(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughIEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> void sendMsgThroughIEdgesArrowProjected(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      int arg3, @FFISkip UNUSED_T arg4) {
    if (arg4 == null) {
      throw new NullPointerException("arg4 must not be null");
    }
    if (arg4 instanceof UnusedImpl.DoubleLongDouble)  {
      sendMsgThroughIEdgesArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (java.lang.Double) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble) arg4); return;
    }
    if (arg4 instanceof UnusedImpl.LongLongDouble)  {
      sendMsgThroughIEdgesArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (java.lang.Double) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble) arg4); return;
    }
    if (arg4 instanceof UnusedImpl.DoubleIntegerDouble)  {
      sendMsgThroughIEdgesArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Integer>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (java.lang.Double) arg2, arg3, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleIntegerDouble) arg4); return;
    }
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughIEdgesArrowProjected, no template instantiation for the type arguments.");
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
  void sendMsgThroughIEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("double") @CXXReference Double arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongDouble arg4);

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
  void sendMsgThroughIEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("double") @CXXReference Double arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongDouble arg4);

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
  void sendMsgThroughIEdgesArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Integer> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("double") @CXXReference Double arg2, int arg3,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleIntegerDouble arg4);
}

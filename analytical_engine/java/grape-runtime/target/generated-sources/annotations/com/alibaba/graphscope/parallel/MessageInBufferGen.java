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
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.runtime.UnusedImpl;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.lang.Double;
import java.lang.Long;

@FFITypeAlias("grape::MessageInBuffer")
@CXXHead({
    "grape/parallel/message_in_buffer.h",
    "grape/fragment/immutable_edgecut_fragment.h",
    "core/fragment/arrow_projected_fragment.h",
    "core/java/type_alias.h",
    "core/java/java_messages.h"
})
@FFIGen(
    type = "com.alibaba.graphscope.parallel.MessageInBuffer",
    functionTemplates = {
        @FFIFunGen(name = "getPureMessage", returnType = "boolean", parameterTypes = "MSG_T", templates = @CXXTemplate(java = "com.alibaba.graphscope.stdcxx.FFIByteVector", cxx = "std::vector<char>")),
        @FFIFunGen(name = "getMessageArrowProjected", returnType = "boolean", parameterTypes = {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "UNUSED_T"}, templates = {@CXXTemplate(java = {"Long", "Long", "Double", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "com.alibaba.graphscope.ds.EmptyType", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongEmpty"}, cxx = {"int64_t", "uint64_t", "double", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>", "grape::EmptyType", "any"}), @CXXTemplate(java = {"Long", "Long", "Long", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Long,Long>", "com.alibaba.graphscope.parallel.message.LongMsg", "com.alibaba.graphscope.runtime.UnusedImpl.LongLongLong"}, cxx = {"int64_t", "uint64_t", "int64_t", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>", "gs::LongMsg", "any"}), @CXXTemplate(java = {"Long", "Long", "Double", "Long", "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "com.alibaba.graphscope.parallel.message.DoubleMsg", "com.alibaba.graphscope.runtime.UnusedImpl.DoubleIntegerDouble"}, cxx = {"int64_t", "uint64_t", "double", "int64_t", "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>", "gs::DoubleMsg", "any"})})
    }
)
@FFISynthetic("com.alibaba.graphscope.parallel.MessageInBuffer")
public abstract interface MessageInBufferGen extends MessageInBuffer {
  @FFINameAlias("GetMessage")
  default <FRAG_T extends ArrowFragment, MSG_T, UNUSED> boolean getMessage(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2, @FFISkip UNUSED arg3) {
    throw new RuntimeException("Cannot call MessageInBufferGen.getMessage, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <OID_T, VID_T, VDATA_T, EDATA_T, FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>, MSG_T, UNUSED_T> boolean getMessageArrowProjected(
      @CXXReference FRAG_T arg0, @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2,
      @FFISkip UNUSED_T arg3) {
    if (arg2 == null) {
      throw new NullPointerException("arg2 must not be null");
    }
    if (arg2 instanceof EmptyType)  {
      return (boolean) getMessageArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.ds.EmptyType) arg2, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongEmpty) arg3);
    }
    if (arg2 instanceof LongMsg)  {
      return (boolean) getMessageArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.parallel.message.LongMsg) arg2, (com.alibaba.graphscope.runtime.UnusedImpl.LongLongLong) arg3);
    }
    if (arg2 instanceof DoubleMsg)  {
      return (boolean) getMessageArrowProjected((com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>) arg0, (com.alibaba.graphscope.ds.Vertex) arg1, (com.alibaba.graphscope.parallel.message.DoubleMsg) arg2, (com.alibaba.graphscope.runtime.UnusedImpl.DoubleIntegerDouble) arg3);
    }
    throw new RuntimeException("Cannot call MessageInBufferGen.getMessageArrowProjected, no template instantiation for the type arguments.");
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
  boolean getMessageArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("grape::EmptyType") @CXXReference EmptyType arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleLongEmpty arg3);

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
  boolean getMessageArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Long, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::LongMsg") @CXXReference LongMsg arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.LongLongLong arg3);

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
  boolean getMessageArrowProjected(
      @FFITypeAlias("gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>") @CXXReference ArrowProjectedFragment<Long, Long, Double, Long> arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1,
      @FFITypeAlias("gs::DoubleMsg") @CXXReference DoubleMsg arg2,
      @FFITypeAlias("any") @FFISkip UnusedImpl.DoubleIntegerDouble arg3);

  @FFINameAlias("GetMessage")
  default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T, UNUSED_T> boolean getMessageImmutable(
      @CXXReference ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> arg0,
      @CXXReference Vertex<VID_T> arg1, @CXXReference MSG_T arg2, @FFISkip UNUSED_T arg3) {
    throw new RuntimeException("Cannot call MessageInBufferGen.getMessageImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <MSG_T> boolean getPureMessage(@CXXReference MSG_T arg0) {
    if (arg0 == null) {
      throw new NullPointerException("arg0 must not be null");
    }
    if (arg0 instanceof FFIByteVector)  {
      return (boolean) getPureMessage((com.alibaba.graphscope.stdcxx.FFIByteVector) arg0);
    }
    throw new RuntimeException("Cannot call MessageInBufferGen.getPureMessage, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  boolean getPureMessage(@FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg0);
}

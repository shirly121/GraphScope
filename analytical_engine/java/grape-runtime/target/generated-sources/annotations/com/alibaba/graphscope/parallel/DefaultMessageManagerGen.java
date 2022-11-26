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
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.lang.Long;

@FFITypeAlias("grape::DefaultMessageManager")
@CXXHead({
    "grape/graph/adj_list.h",
    "grape/parallel/default_message_manager.h",
    "grape/fragment/immutable_edgecut_fragment.h",
    "core/fragment/arrow_projected_fragment.h",
    "core/java/java_messages.h"
})
@FFIGen(
    type = "com.alibaba.graphscope.parallel.DefaultMessageManager",
    functionTemplates = {
        @FFIFunGen(name = "sendToFragment", returnType = "void", parameterTypes = {"int", "MSG_T"}, templates = @CXXTemplate(java = "com.alibaba.graphscope.stdcxx.FFIByteVector", cxx = "std::vector<char>")),
        @FFIFunGen(name = "getPureMessage", returnType = "boolean", parameterTypes = "MSG_T", templates = @CXXTemplate(java = "com.alibaba.graphscope.stdcxx.FFIByteVector", cxx = "std::vector<char>"))
    }
)
@FFISynthetic("com.alibaba.graphscope.parallel.DefaultMessageManager")
public abstract interface DefaultMessageManagerGen extends DefaultMessageManager {
  @FFINameAlias("SendToFragment")
  default <MSG_T> void sendToFragment(int arg0, @CXXReference MSG_T arg1) {
    if (arg1 == null) {
      throw new NullPointerException("arg1 must not be null");
    }
    if (arg1 instanceof FFIByteVector)  {
      sendToFragment(arg0, (com.alibaba.graphscope.stdcxx.FFIByteVector) arg1); return;
    }
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendToFragment, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendToFragment")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  void sendToFragment(int arg0,
      @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg1);

  @FFINameAlias("GetMessage")
  default <MSG_T> boolean getPureMessage(@CXXReference MSG_T arg0) {
    if (arg0 == null) {
      throw new NullPointerException("arg0 must not be null");
    }
    if (arg0 instanceof FFIByteVector)  {
      return (boolean) getPureMessage((com.alibaba.graphscope.stdcxx.FFIByteVector) arg0);
    }
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.getPureMessage, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  boolean getPureMessage(@FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg0);

  @FFINameAlias("GetMessage")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> boolean getMessageImmutable(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.getMessageImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, SKIP_T> boolean getMessageArrowProjected(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2, @FFISkip SKIP_T arg3) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.getMessageArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void syncStateOnOuterVertexImmutable(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.syncStateOnOuterVertexImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void syncStateOnOuterVertexArrowProjected(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.syncStateOnOuterVertexArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughOEdgesImmutable(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughOEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughOEdgesArrowProjected(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughOEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughIEdgesImmutable(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughIEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughIEdgesArrowProjected(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughIEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughEdgesImmutable(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughEdgesArrowProjected(
      @CXXReference FRAG_T arg0,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg1,
      @CXXReference MSG_T arg2) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughEdgesArrowProjected, no template instantiation for the type arguments.");
  }
}

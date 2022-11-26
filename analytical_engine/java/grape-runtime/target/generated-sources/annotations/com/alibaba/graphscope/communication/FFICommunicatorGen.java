package com.alibaba.graphscope.communication;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXHeads;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIFunGen;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.FFIByteVector;

@CXXHeads({
    @CXXHead("grape/communication/communicator.h"),
    @CXXHead("core/java/java_messages.h")
})
@FFITypeAlias("grape::Communicator")
@FFIGen(
    type = "com.alibaba.graphscope.communication.FFICommunicator",
    functionTemplates = {
        @FFIFunGen(name = "sum", returnType = "void", parameterTypes = "MSG_T", templates = {@CXXTemplate(java = "com.alibaba.graphscope.parallel.message.DoubleMsg", cxx = "gs::DoubleMsg"), @CXXTemplate(java = "com.alibaba.graphscope.parallel.message.LongMsg", cxx = "gs::LongMsg")}),
        @FFIFunGen(name = "min", returnType = "void", parameterTypes = "MSG_T", templates = {@CXXTemplate(java = "com.alibaba.graphscope.parallel.message.DoubleMsg", cxx = "gs::DoubleMsg"), @CXXTemplate(java = "com.alibaba.graphscope.parallel.message.LongMsg", cxx = "gs::LongMsg")}),
        @FFIFunGen(name = "max", returnType = "void", parameterTypes = "MSG_T", templates = {@CXXTemplate(java = "com.alibaba.graphscope.parallel.message.DoubleMsg", cxx = "gs::DoubleMsg"), @CXXTemplate(java = "com.alibaba.graphscope.parallel.message.LongMsg", cxx = "gs::LongMsg")}),
        @FFIFunGen(name = "sendTo", returnType = "void", parameterTypes = {"int", "MSG_T"}, templates = @CXXTemplate(java = "com.alibaba.graphscope.stdcxx.FFIByteVector", cxx = "std::vector<char>")),
        @FFIFunGen(name = "receiveFrom", returnType = "void", parameterTypes = {"int", "MSG_T"}, templates = @CXXTemplate(java = "com.alibaba.graphscope.stdcxx.FFIByteVector", cxx = "std::vector<char>"))
    }
)
@FFISynthetic("com.alibaba.graphscope.communication.FFICommunicator")
public abstract interface FFICommunicatorGen extends FFICommunicator {
  @FFINameAlias("Sum")
  default <MSG_T> void sum(@FFIConst @CXXReference MSG_T arg0, @CXXReference MSG_T arg1) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.sum, no template instantiation for the type arguments.");
  }

  @FFINameAlias("Min")
  default <MSG_T> void min(@FFIConst @CXXReference MSG_T arg0, @CXXReference MSG_T arg1) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.min, no template instantiation for the type arguments.");
  }

  @FFINameAlias("Max")
  default <MSG_T> void max(@FFIConst @CXXReference MSG_T arg0, @CXXReference MSG_T arg1) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.max, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendTo")
  default <MST_T> void sendTo(int arg0, @FFIConst @CXXReference MST_T arg1) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.sendTo, no template instantiation for the type arguments.");
  }

  @FFINameAlias("RecvFrom")
  default <MSG_T> void receiveFrom(int arg0, @CXXReference MSG_T arg1) {
    if (arg1 == null) {
      throw new NullPointerException("arg1 must not be null");
    }
    if (arg1 instanceof FFIByteVector)  {
      receiveFrom(arg0, (com.alibaba.graphscope.stdcxx.FFIByteVector) arg1); return;
    }
    throw new RuntimeException("Cannot call FFICommunicatorGen.receiveFrom, no template instantiation for the type arguments.");
  }

  @FFINameAlias("RecvFrom")
  @CXXTemplate(
      cxx = "std::vector<char>",
      java = "com.alibaba.graphscope.stdcxx.FFIByteVector"
  )
  void receiveFrom(int arg0, @FFITypeAlias("std::vector<char>") @CXXReference FFIByteVector arg1);
}

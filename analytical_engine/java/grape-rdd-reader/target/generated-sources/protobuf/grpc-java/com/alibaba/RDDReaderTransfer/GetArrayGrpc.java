package com.alibaba.RDDReaderTransfer;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.47.0)",
    comments = "Source: rdd.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class GetArrayGrpc {

  private GetArrayGrpc() {}

  public static final String SERVICE_NAME = "RDDReaderTransfer.GetArray";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.PartInfoRequest,
      com.alibaba.RDDReaderTransfer.PartitionInfo> getGetPartitionInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionInfo",
      requestType = com.alibaba.RDDReaderTransfer.PartInfoRequest.class,
      responseType = com.alibaba.RDDReaderTransfer.PartitionInfo.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.PartInfoRequest,
      com.alibaba.RDDReaderTransfer.PartitionInfo> getGetPartitionInfoMethod() {
    io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.PartInfoRequest, com.alibaba.RDDReaderTransfer.PartitionInfo> getGetPartitionInfoMethod;
    if ((getGetPartitionInfoMethod = GetArrayGrpc.getGetPartitionInfoMethod) == null) {
      synchronized (GetArrayGrpc.class) {
        if ((getGetPartitionInfoMethod = GetArrayGrpc.getGetPartitionInfoMethod) == null) {
          GetArrayGrpc.getGetPartitionInfoMethod = getGetPartitionInfoMethod =
              io.grpc.MethodDescriptor.<com.alibaba.RDDReaderTransfer.PartInfoRequest, com.alibaba.RDDReaderTransfer.PartitionInfo>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPartitionInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.RDDReaderTransfer.PartInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.RDDReaderTransfer.PartitionInfo.getDefaultInstance()))
              .setSchemaDescriptor(new GetArrayMethodDescriptorSupplier("GetPartitionInfo"))
              .build();
        }
      }
    }
    return getGetPartitionInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.ItemRequest,
      com.alibaba.RDDReaderTransfer.PartitionItem> getGetPartitionItemMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionItem",
      requestType = com.alibaba.RDDReaderTransfer.ItemRequest.class,
      responseType = com.alibaba.RDDReaderTransfer.PartitionItem.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.ItemRequest,
      com.alibaba.RDDReaderTransfer.PartitionItem> getGetPartitionItemMethod() {
    io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.ItemRequest, com.alibaba.RDDReaderTransfer.PartitionItem> getGetPartitionItemMethod;
    if ((getGetPartitionItemMethod = GetArrayGrpc.getGetPartitionItemMethod) == null) {
      synchronized (GetArrayGrpc.class) {
        if ((getGetPartitionItemMethod = GetArrayGrpc.getGetPartitionItemMethod) == null) {
          GetArrayGrpc.getGetPartitionItemMethod = getGetPartitionItemMethod =
              io.grpc.MethodDescriptor.<com.alibaba.RDDReaderTransfer.ItemRequest, com.alibaba.RDDReaderTransfer.PartitionItem>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPartitionItem"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.RDDReaderTransfer.ItemRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.RDDReaderTransfer.PartitionItem.getDefaultInstance()))
              .setSchemaDescriptor(new GetArrayMethodDescriptorSupplier("GetPartitionItem"))
              .build();
        }
      }
    }
    return getGetPartitionItemMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.CloseRequest,
      com.alibaba.RDDReaderTransfer.CloseResponse> getRpcCloseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RpcClose",
      requestType = com.alibaba.RDDReaderTransfer.CloseRequest.class,
      responseType = com.alibaba.RDDReaderTransfer.CloseResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.CloseRequest,
      com.alibaba.RDDReaderTransfer.CloseResponse> getRpcCloseMethod() {
    io.grpc.MethodDescriptor<com.alibaba.RDDReaderTransfer.CloseRequest, com.alibaba.RDDReaderTransfer.CloseResponse> getRpcCloseMethod;
    if ((getRpcCloseMethod = GetArrayGrpc.getRpcCloseMethod) == null) {
      synchronized (GetArrayGrpc.class) {
        if ((getRpcCloseMethod = GetArrayGrpc.getRpcCloseMethod) == null) {
          GetArrayGrpc.getRpcCloseMethod = getRpcCloseMethod =
              io.grpc.MethodDescriptor.<com.alibaba.RDDReaderTransfer.CloseRequest, com.alibaba.RDDReaderTransfer.CloseResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RpcClose"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.RDDReaderTransfer.CloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.RDDReaderTransfer.CloseResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GetArrayMethodDescriptorSupplier("RpcClose"))
              .build();
        }
      }
    }
    return getRpcCloseMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GetArrayStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GetArrayStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GetArrayStub>() {
        @java.lang.Override
        public GetArrayStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GetArrayStub(channel, callOptions);
        }
      };
    return GetArrayStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GetArrayBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GetArrayBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GetArrayBlockingStub>() {
        @java.lang.Override
        public GetArrayBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GetArrayBlockingStub(channel, callOptions);
        }
      };
    return GetArrayBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GetArrayFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GetArrayFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GetArrayFutureStub>() {
        @java.lang.Override
        public GetArrayFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GetArrayFutureStub(channel, callOptions);
        }
      };
    return GetArrayFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class GetArrayImplBase implements io.grpc.BindableService {

    /**
     */
    public void getPartitionInfo(com.alibaba.RDDReaderTransfer.PartInfoRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.PartitionInfo> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPartitionInfoMethod(), responseObserver);
    }

    /**
     */
    public void getPartitionItem(com.alibaba.RDDReaderTransfer.ItemRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.PartitionItem> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPartitionItemMethod(), responseObserver);
    }

    /**
     */
    public void rpcClose(com.alibaba.RDDReaderTransfer.CloseRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.CloseResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRpcCloseMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetPartitionInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.RDDReaderTransfer.PartInfoRequest,
                com.alibaba.RDDReaderTransfer.PartitionInfo>(
                  this, METHODID_GET_PARTITION_INFO)))
          .addMethod(
            getGetPartitionItemMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.RDDReaderTransfer.ItemRequest,
                com.alibaba.RDDReaderTransfer.PartitionItem>(
                  this, METHODID_GET_PARTITION_ITEM)))
          .addMethod(
            getRpcCloseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.RDDReaderTransfer.CloseRequest,
                com.alibaba.RDDReaderTransfer.CloseResponse>(
                  this, METHODID_RPC_CLOSE)))
          .build();
    }
  }

  /**
   */
  public static final class GetArrayStub extends io.grpc.stub.AbstractAsyncStub<GetArrayStub> {
    private GetArrayStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetArrayStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GetArrayStub(channel, callOptions);
    }

    /**
     */
    public void getPartitionInfo(com.alibaba.RDDReaderTransfer.PartInfoRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.PartitionInfo> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPartitionInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPartitionItem(com.alibaba.RDDReaderTransfer.ItemRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.PartitionItem> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetPartitionItemMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rpcClose(com.alibaba.RDDReaderTransfer.CloseRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.CloseResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRpcCloseMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GetArrayBlockingStub extends io.grpc.stub.AbstractBlockingStub<GetArrayBlockingStub> {
    private GetArrayBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetArrayBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GetArrayBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.RDDReaderTransfer.PartitionInfo getPartitionInfo(com.alibaba.RDDReaderTransfer.PartInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPartitionInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.RDDReaderTransfer.PartitionItem> getPartitionItem(
        com.alibaba.RDDReaderTransfer.ItemRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetPartitionItemMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.RDDReaderTransfer.CloseResponse rpcClose(com.alibaba.RDDReaderTransfer.CloseRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRpcCloseMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GetArrayFutureStub extends io.grpc.stub.AbstractFutureStub<GetArrayFutureStub> {
    private GetArrayFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GetArrayFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GetArrayFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.RDDReaderTransfer.PartitionInfo> getPartitionInfo(
        com.alibaba.RDDReaderTransfer.PartInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPartitionInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.RDDReaderTransfer.CloseResponse> rpcClose(
        com.alibaba.RDDReaderTransfer.CloseRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRpcCloseMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_PARTITION_INFO = 0;
  private static final int METHODID_GET_PARTITION_ITEM = 1;
  private static final int METHODID_RPC_CLOSE = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GetArrayImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GetArrayImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_PARTITION_INFO:
          serviceImpl.getPartitionInfo((com.alibaba.RDDReaderTransfer.PartInfoRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.PartitionInfo>) responseObserver);
          break;
        case METHODID_GET_PARTITION_ITEM:
          serviceImpl.getPartitionItem((com.alibaba.RDDReaderTransfer.ItemRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.PartitionItem>) responseObserver);
          break;
        case METHODID_RPC_CLOSE:
          serviceImpl.rpcClose((com.alibaba.RDDReaderTransfer.CloseRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.RDDReaderTransfer.CloseResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class GetArrayBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GetArrayBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.alibaba.RDDReaderTransfer.RDDReaderTransferProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GetArray");
    }
  }

  private static final class GetArrayFileDescriptorSupplier
      extends GetArrayBaseDescriptorSupplier {
    GetArrayFileDescriptorSupplier() {}
  }

  private static final class GetArrayMethodDescriptorSupplier
      extends GetArrayBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GetArrayMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GetArrayGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GetArrayFileDescriptorSupplier())
              .addMethod(getGetPartitionInfoMethod())
              .addMethod(getGetPartitionItemMethod())
              .addMethod(getRpcCloseMethod())
              .build();
        }
      }
    }
    return result;
  }
}

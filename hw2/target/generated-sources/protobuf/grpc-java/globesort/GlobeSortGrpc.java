package globesort;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.6.1)",
    comments = "Source: globesort.proto")
public final class GlobeSortGrpc {

  private GlobeSortGrpc() {}

  public static final String SERVICE_NAME = "GlobeSort";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<globesort.Empty,
      globesort.Empty> METHOD_PING =
      io.grpc.MethodDescriptor.<globesort.Empty, globesort.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "GlobeSort", "Ping"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              globesort.Empty.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              globesort.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<globesort.IntArray,
      globesort.IntArray> METHOD_SORT_INTEGERS =
      io.grpc.MethodDescriptor.<globesort.IntArray, globesort.IntArray>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "GlobeSort", "SortIntegers"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              globesort.IntArray.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              globesort.IntArray.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GlobeSortStub newStub(io.grpc.Channel channel) {
    return new GlobeSortStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GlobeSortBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GlobeSortBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GlobeSortFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GlobeSortFutureStub(channel);
  }

  /**
   */
  public static abstract class GlobeSortImplBase implements io.grpc.BindableService {

    /**
     */
    public void ping(globesort.Empty request,
        io.grpc.stub.StreamObserver<globesort.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PING, responseObserver);
    }

    /**
     */
    public void sortIntegers(globesort.IntArray request,
        io.grpc.stub.StreamObserver<globesort.IntArray> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SORT_INTEGERS, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_PING,
            asyncUnaryCall(
              new MethodHandlers<
                globesort.Empty,
                globesort.Empty>(
                  this, METHODID_PING)))
          .addMethod(
            METHOD_SORT_INTEGERS,
            asyncUnaryCall(
              new MethodHandlers<
                globesort.IntArray,
                globesort.IntArray>(
                  this, METHODID_SORT_INTEGERS)))
          .build();
    }
  }

  /**
   */
  public static final class GlobeSortStub extends io.grpc.stub.AbstractStub<GlobeSortStub> {
    private GlobeSortStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GlobeSortStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GlobeSortStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GlobeSortStub(channel, callOptions);
    }

    /**
     */
    public void ping(globesort.Empty request,
        io.grpc.stub.StreamObserver<globesort.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PING, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sortIntegers(globesort.IntArray request,
        io.grpc.stub.StreamObserver<globesort.IntArray> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SORT_INTEGERS, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GlobeSortBlockingStub extends io.grpc.stub.AbstractStub<GlobeSortBlockingStub> {
    private GlobeSortBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GlobeSortBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GlobeSortBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GlobeSortBlockingStub(channel, callOptions);
    }

    /**
     */
    public globesort.Empty ping(globesort.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PING, getCallOptions(), request);
    }

    /**
     */
    public globesort.IntArray sortIntegers(globesort.IntArray request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SORT_INTEGERS, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GlobeSortFutureStub extends io.grpc.stub.AbstractStub<GlobeSortFutureStub> {
    private GlobeSortFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GlobeSortFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GlobeSortFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GlobeSortFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<globesort.Empty> ping(
        globesort.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PING, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<globesort.IntArray> sortIntegers(
        globesort.IntArray request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SORT_INTEGERS, getCallOptions()), request);
    }
  }

  private static final int METHODID_PING = 0;
  private static final int METHODID_SORT_INTEGERS = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GlobeSortImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GlobeSortImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PING:
          serviceImpl.ping((globesort.Empty) request,
              (io.grpc.stub.StreamObserver<globesort.Empty>) responseObserver);
          break;
        case METHODID_SORT_INTEGERS:
          serviceImpl.sortIntegers((globesort.IntArray) request,
              (io.grpc.stub.StreamObserver<globesort.IntArray>) responseObserver);
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

  private static final class GlobeSortDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return globesort.GlobeSortProto.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GlobeSortGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GlobeSortDescriptorSupplier())
              .addMethod(METHOD_PING)
              .addMethod(METHOD_SORT_INTEGERS)
              .build();
        }
      }
    }
    return result;
  }
}

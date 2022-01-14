

package com.google.solutions.dataflow.grpc.pipeline;

import com.google.solutions.dataflow.grpc.ResolveRequest;
import com.google.solutions.dataflow.grpc.ResolveResponse;
import com.google.solutions.dataflow.grpc.ResolvedAddress;
import com.google.solutions.dataflow.grpc.ZipResolverGrpc;
import com.google.solutions.dataflow.grpc.ZipResolverGrpc.ZipResolverStub;
import com.google.solutions.dataflow.grpc.model.PartialAddress;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipResolverDoFn extends DoFn<String, PartialAddress> {

  private static final Logger logger = LoggerFactory.getLogger(ZipResolverDoFn.class);

  public static final TupleTag<PartialAddress> successfullyResolvedTag = new TupleTag<>() {
  };
  public static final TupleTag<KV<String, String>> failedToResolveTag = new TupleTag<>() {
  };

  public static final Distribution grpcDurationMetric = Metrics
      .distribution(ZipResolverDoFn.class, "grpcDuration");

  public static final Distribution bundleSizeMetric = Metrics
      .distribution(ZipResolverDoFn.class, "bundleSize");

  private ZipResolverStub stub;
  private ManagedChannel channel;
  private int bundleSize;
  private AtomicBoolean allRequestsQueued;
  private Queue<RequestElement> requestQueue;
  private Collection<ResponseElement> successfulResponses;
  private Collection<FailureElement> failureResponses;

  private CountDownLatch allResponsesProcessed;
  private StreamObserver<ResolveRequest> streamObserver;

  private Map<String, BundleElement> bundleElementsByRequestId;

  static class BundleElement {

    String zip;
    Instant timestamp;
    BoundedWindow window;
  }

  static class RequestElement {

    String requestId;
    String zip;
  }

  static class ResponseElement {

    String requestId;
    ResolvedAddress resolvedAddress;
  }

  static class FailureElement {

    String requestId;
    String failureReason;
  }

  @Setup
  public void setup() {
    // Create a channel and a stub
    channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()
        .build();
    stub = ZipResolverGrpc.newStub(channel);
  }

  @StartBundle
  public void startBundle() {
    allRequestsQueued = new AtomicBoolean(false);
    requestQueue = new ConcurrentLinkedQueue<>();
    successfulResponses = new ArrayList<>();
    failureResponses = new ArrayList<>();
    allResponsesProcessed = new CountDownLatch(1);
    bundleElementsByRequestId = new ConcurrentHashMap<>();

    // When using manual flow-control and back-pressure on the client, the ClientResponseObserver handles both
    // request and response streams.
    ClientResponseObserver<ResolveRequest, ResolveResponse> clientResponseObserver =
        new ClientResponseObserver<>() {

          ClientCallStreamObserver<ResolveRequest> requestStream;

          @Override
          public void beforeStart(final ClientCallStreamObserver<ResolveRequest> requestStream) {
            this.requestStream = requestStream;
            // Set up manual flow control for the response stream. It feels backwards to configure the response
            // stream's flow control using the request stream's observer, but this is the way it is.
            // TODO: check how the parameter affects performance.
            requestStream.disableAutoRequestWithInitial(1);

            // Set up a back-pressure-aware producer for the request stream. The onReadyHandler will be invoked
            // when the consuming side has enough buffer space to receive more messages.
            //
            // Messages are serialized into a transport-specific transmit buffer. Depending on the size of this buffer,
            // MANY messages may be buffered, however, they haven't yet been sent to the server. The server must call
            // request() to pull a buffered message from the client.
            //
            // Note: the onReadyHandler's invocation is serialized on the same thread pool as the incoming
            // StreamObserver's onNext(), onError(), and onComplete() handlers. Blocking the onReadyHandler will prevent
            // additional messages from being processed by the incoming StreamObserver. The onReadyHandler must return
            // in a timely manner or else message processing throughput will suffer.
            requestStream.setOnReadyHandler(() -> {
              // Start generating values from where we left off on a non-gRPC thread.
              while (requestStream.isReady()) {
                RequestElement requestElement = requestQueue.poll();
                if (requestElement == null) {
                  if (allRequestsQueued.get()) {
                    // Signal completion if there is nothing left to send.
                    requestStream.onCompleted();
                  } else {
                    // Not all the requests are queued yet.
                    break;
                  }
                } else {
                  logger.info("--> " + requestElement.requestId + " " + requestElement.zip);
                  ResolveRequest request = ResolveRequest.newBuilder()
                      .setRequestId(requestElement.requestId)
                      .setZip(requestElement.zip).build();
                  requestStream.onNext(request);
                }
              }
            });
          }

          @Override
          public void onNext(ResolveResponse response) {
            logger.info("<-- " + response.getOutcome() + " " + response.getResolvedAddress());

            switch (response.getOutcome()) {
              case success:
                ResponseElement responseElement = new ResponseElement();
                responseElement.requestId = response.getRequestId();
                responseElement.resolvedAddress = response.getResolvedAddress();
                successfulResponses.add(responseElement);
                break;

              case failure:
                FailureElement e = new FailureElement();
                e.requestId = response.getRequestId();
                e.failureReason = response.getFailureReason();
                failureResponses.add(e);
                break;

              default:
                throw new RuntimeException(
                    "Unexpected outcome of the gRPC call: " + response.getOutcome());
            }

            // Signal the sender to send one message. TODO: test increasing this response
            requestStream.request(1);
          }

          @Override
          public void onError(Throwable e) {
            // TODO: figure out if the pipeline should fail if this happens.
            logger.error("Failed to process requests", e);
            allResponsesProcessed.countDown();
          }

          @Override
          public void onCompleted() {
            logger.info("All Done");
            allResponsesProcessed.countDown();
          }
        };

    // Note: clientResponseObserver is handling both request and response stream processing.
    // TODO: do we need to close the streamObserver?
    streamObserver = stub.zipResolverStreaming(clientResponseObserver);
  }

  @ProcessElement
  public void processElement(@Element String zip, BoundedWindow window,
      @Timestamp Instant timestamp) {
    ++bundleSize;
    String requestId = UUID.randomUUID().toString();
    RequestElement requeestElement = new RequestElement();
    requeestElement.requestId = requestId;
    requeestElement.zip = zip;

    requestQueue.add(requeestElement);

    BundleElement bundleElement = new BundleElement();
    bundleElement.zip = zip;
    bundleElement.timestamp = timestamp;
    bundleElement.window = window;

    bundleElementsByRequestId.put(requestId, bundleElement);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    allRequestsQueued = new AtomicBoolean(true);
    bundleSizeMetric.update(bundleSize);

    while (true) {
      try {
        allResponsesProcessed.await();
        break;
      } catch (InterruptedException e) {
        logger.warn("Unexpected exception: ", e);
      }
    }

    successfulResponses.forEach(responseElement -> {
          String requestId = responseElement.requestId;
          ResolvedAddress response = responseElement.resolvedAddress;
          BundleElement bundleElement = bundleElementsByRequestId.get(requestId);
          if (bundleElement == null) {
            throw new RuntimeException("Unable to find bundle element by id: " + requestId);
          }

          context.output(successfullyResolvedTag,
              PartialAddress.create(bundleElement.zip, response.getState(), response.getCity()),
              bundleElement.timestamp, bundleElement.window
          );
        }
    );

    failureResponses.forEach(failedResponse -> {
      String requestId = failedResponse.requestId;
      String failureReason = failedResponse.failureReason;
      BundleElement bundleElement = bundleElementsByRequestId.get(requestId);
      if (bundleElement == null) {
        throw new RuntimeException("Unable to find bundle element by id: " + requestId);
      }
      context.output(failedToResolveTag, KV.of(bundleElement.zip, failureReason),
          bundleElement.timestamp, bundleElement.window);
    });

  }

  @Teardown
  public void tearDown() {
    channel.shutdown();
    try {
      channel.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while terminating gRPC channel:", e);
    }
  }

}

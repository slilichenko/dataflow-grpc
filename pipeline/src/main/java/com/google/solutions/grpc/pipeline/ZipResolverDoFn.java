

/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.grpc.pipeline;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.solutions.grpc.pipeline.model.PartialAddress;
import com.google.solutions.grpc.ResolveRequest;
import com.google.solutions.grpc.ResolveResponse;
import com.google.solutions.grpc.ZipResolverGrpc;
import com.google.solutions.grpc.ZipResolverGrpc.ZipResolverStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoFn to convert a US Postal Service ZIP code into a ${@link PartialAddress} with the ZIP, state
 * abbreviation and the city.
 *
 * It uses a gRPC service to look up the data. There are two outputs: - PCollection of resolved
 * ${@link PartialAddress}es - PCollection of failed lookups as ${@link KV} of ZIP and failure
 * reason
 */
public class ZipResolverDoFn extends DoFn<String, PartialAddress> {

  private static final Logger logger = LoggerFactory.getLogger(ZipResolverDoFn.class);

  /*
   * Output tags
   */
  public static final TupleTag<PartialAddress> successfullyResolvedTag = new TupleTag<>() {
  };
  public static final TupleTag<KV<String, String>> failedToResolveTag = new TupleTag<>() {
  };

  public static final Distribution grpcDurationMetric = Metrics
      .distribution(ZipResolverDoFn.class, "grpc-duration-ms");

  public static final Distribution bundleSizeMetric = Metrics
      .distribution(ZipResolverDoFn.class, "bundle-size");

  public static final Counter gRPCFailures = Metrics
      .counter(ZipResolverDoFn.class, "grpc-failures");

  private ManagedChannel channel;
  private StreamObserver<ResolveRequest> streamObserver;

  private int bundleSize;
  private Stopwatch stopwatch;

  private AtomicBoolean allRequestsQueued;
  private Queue<ResolveRequest> requests;

  private Collection<ResolveResponse> responses;
  private CountDownLatch allResponsesProcessed;
  private AtomicBoolean failureProcessing;

  private Map<String, PCollectionElement> bundleElementsByRequestId;

  private final String gRPCServerHost;
  private final int gRPCServerPort;
  private final boolean gRPCUsePlainText;
  private final int timeoutSeconds;

  public ZipResolverDoFn(String gRPCServerHost, int gRPCServerPort, boolean usePlainText,
      int timeoutSeconds) {
    this.gRPCServerHost = gRPCServerHost;
    this.gRPCServerPort = gRPCServerPort;
    this.gRPCUsePlainText = usePlainText;
    this.timeoutSeconds = timeoutSeconds;
  }

  static class PCollectionElement {

    String zip;
    Instant timestamp;
    BoundedWindow window;
  }

  @Setup
  public void setup() {
    // Create a channel
    ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder
        .forAddress(gRPCServerHost, gRPCServerPort);
    if(gRPCUsePlainText) {
      managedChannelBuilder = managedChannelBuilder.usePlaintext();
    }

    Map<String, ?> serviceConfig = getRetryingServiceConfig();

    managedChannelBuilder = managedChannelBuilder.defaultServiceConfig(serviceConfig).enableRetry();
    channel = managedChannelBuilder.build();
  }

  protected Map<String, ?> getRetryingServiceConfig() {
    InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(
        "retrying_service_config.json");
    return new Gson()
        .fromJson(
            new JsonReader(new InputStreamReader(resourceAsStream, UTF_8)),
            Map.class);
  }

  @StartBundle
  public void startBundle() {
    logger.debug("Starting bundle processing.");
    allRequestsQueued = new AtomicBoolean(false);
    requests = new ConcurrentLinkedQueue<>();

    allResponsesProcessed = new CountDownLatch(1);
    responses = new ArrayList<>();
    failureProcessing = new AtomicBoolean(false);

    bundleElementsByRequestId = new ConcurrentHashMap<>();

    stopwatch = Stopwatch.createStarted();

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
                ResolveRequest request = requests.poll();
                if (request == null) {
                  if (allRequestsQueued.get()) {
                    // Signals completion if there is nothing left to send.
                    logger.debug("All requests are sent. Completing the request stream.");
                    requestStream.onCompleted();
                  } else {
                    // More requests are going to be queued. We shouldn't block the thread and return.
                    logger.debug("No more items in the queue, but more requests are expected.");
                    break;
                  }
                } else {
                  logger.debug("Requesting " + request);
                  requestStream.onNext(request);
                }
              }
            });
          }

          @Override
          public void onNext(ResolveResponse response) {
            logger.debug("<-- " + response.getOutcome() + " " + response.getResolvedAddress());
            responses.add(response);
            // Signal the sender to send one message. TODO: test increasing this response
            requestStream.request(1);
          }

          @Override
          public void onError(Throwable e) {
            // TODO: figure out if the pipeline should fail if this happens.
            logger.error("Failed to process requests", e);
            failureProcessing.set(true);
            allResponsesProcessed.countDown();
          }

          @Override
          public void onCompleted() {
            logger.debug("gRPC processing is complete.");
            allResponsesProcessed.countDown();
          }
        };

    // Note: clientResponseObserver is handling both request and response stream processing.
    // TODO: do we need to close the streamObserver?
    ZipResolverStub stub = ZipResolverGrpc.newStub(channel);
    // TODO: shall we deal with deadlines?
    // .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    stub.zipResolverStreaming(clientResponseObserver);
  }

  @ProcessElement
  public void processElement(@Element String zip, BoundedWindow window,
      @Timestamp Instant timestamp) {
    ++bundleSize;
    var requestId = UUID.randomUUID().toString();
    logger.debug("Generated request: " + requestId);
    var request = ResolveRequest.newBuilder()
        .setRequestId(requestId)
        .setZip(zip).build();

    requests.add(request);

    var bundleElement = new PCollectionElement();
    bundleElement.zip = zip;
    bundleElement.timestamp = timestamp;
    bundleElement.window = window;

    bundleElementsByRequestId.put(requestId, bundleElement);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    logger.debug("Bundle requests are queued. Waiting for the gRPC server to finish responding.");
    allRequestsQueued = new AtomicBoolean(true);
    bundleSizeMetric.update(bundleSize);

    while (true) {
      try {
        boolean success = allResponsesProcessed.await(timeoutSeconds * bundleSize, TimeUnit.SECONDS);
        if(success) {
          break;
        } else {
          throw new RuntimeException("Timeout exceeded");
        }
      } catch (InterruptedException e) {
        logger.warn("Unexpected exception: ", e);
      }
    }

    if(failureProcessing.get()) {
      gRPCFailures.inc();
      throw new RuntimeException("Failed to process the bundle.");
    }

    logger.debug("gRPC processing is completed.");
    stopwatch.stop();
    grpcDurationMetric.update(stopwatch.elapsed(TimeUnit.MILLISECONDS) / bundleSize);

    responses.forEach(response -> {
          var requestId = response.getRequestId();
          var pCollectionElement = bundleElementsByRequestId.get(requestId);
          if (pCollectionElement == null) {
            throw new RuntimeException("Unable to find bundle element by id: " + requestId);
          }

          switch (response.getOutcome()) {
            case success:
              var resolvedAddress = response.getResolvedAddress();
              context.output(successfullyResolvedTag,
                  PartialAddress.create(pCollectionElement.zip, resolvedAddress.getState(),
                      resolvedAddress.getCity()),
                  pCollectionElement.timestamp, pCollectionElement.window
              );
              break;

            case failure:
              context.output(failedToResolveTag,
                  KV.of(pCollectionElement.zip, response.getFailureReason()),
                  pCollectionElement.timestamp, pCollectionElement.window);
              break;

            default:
              throw new RuntimeException(
                  "Unexpected outcome of the gRPC call: " + response.getOutcome());
          }
        }
    );
  }

  @Teardown
  public void tearDown() {
    if(channel == null) {
      return;
    }
    channel.shutdown();
    try {
      channel.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while terminating gRPC channel:", e);
    }
  }

}

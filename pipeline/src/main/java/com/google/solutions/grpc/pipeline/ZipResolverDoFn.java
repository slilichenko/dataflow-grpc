

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
import com.google.solutions.grpc.GRPCBidiCallProcessor;
import com.google.solutions.grpc.GRPCBidiCallProcessor.BidiMethodStarter;
import com.google.solutions.grpc.GRPCBidiCallProcessor.ResponseReceiver;
import com.google.solutions.grpc.ResolveRequest;
import com.google.solutions.grpc.ResolveResponse;
import com.google.solutions.grpc.ZipResolverGrpc;
import com.google.solutions.grpc.ZipResolverGrpc.ZipResolverStub;
import com.google.solutions.grpc.pipeline.model.PartialAddress;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
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

  private static ManagedChannel channel;
  private static final Lock channelLock = new ReentrantLock();
  private static int numberOfChannelUsers = 0;

  private int bundleSize;
  private Stopwatch stopwatch;

  private Queue<ResolveRequest> requests;
  private Collection<ResolveResponse> responses;
  private Map<String, ResolveRequest> unprocessedRequests;
  private Map<String, PCollectionElement> bundleElementsByRequestId;
  private final ResponseReceiver<ResolveResponse> resolveResponseReceiver =
      (ResponseReceiver<ResolveResponse> & Serializable) response -> {
        if (unprocessedRequests.remove(response.getRequestId()) == null) {
          // TODO: more complex error handling - something is very wrong here.
          logger.warn("Failed to find the request by id: " + response.getRequestId());
          return;
        }
        responses.add(response);
      };
  private final GRPCBidiCallProcessor.BidiMethodStarter<ResolveRequest, ResolveResponse> starter =
      (BidiMethodStarter<ResolveRequest, ResolveResponse> & Serializable) (clientResponseObserver) -> {
        // Note: clientResponseObserver is handling both request and response stream processing.
        // TODO: do we need to close the streamObserver?
        ZipResolverStub stub = ZipResolverGrpc.newStub(channel);
        // TODO: shall we deal with deadlines?
        // .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
        stub.zipResolverStreaming(clientResponseObserver);
      };

  private final String gRPCServerHost;
  private final int gRPCServerPort;
  private final boolean gRPCUsePlainText;

  private GRPCBidiCallProcessor<ResolveRequest, ResolveResponse> callProcessor;

  public ZipResolverDoFn(String gRPCServerHost, int gRPCServerPort, boolean usePlainText) {
    this.gRPCServerHost = gRPCServerHost;
    this.gRPCServerPort = gRPCServerPort;
    this.gRPCUsePlainText = usePlainText;
  }

  static class PCollectionElement {

    String zip;
    Instant timestamp;
    BoundedWindow window;
  }

  @Setup
  public void setup() {
    channelLock.lock();
    try {
      numberOfChannelUsers++;

      if (channel != null) {
        logger.debug("Reusing an existing gRPC channel for " + this.getClass().getName());
        return;
      }
      logger.info("Creating a gRPC channel for " + this.getClass().getName());
      ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder
          .forAddress(gRPCServerHost, gRPCServerPort);
      if (gRPCUsePlainText) {
        managedChannelBuilder = managedChannelBuilder.usePlaintext();
      }

      Map<String, ?> serviceConfig = getRetryingServiceConfig();

      managedChannelBuilder = managedChannelBuilder.defaultServiceConfig(serviceConfig)
          .enableRetry();
      channel = managedChannelBuilder.build();
    } finally {
      channelLock.unlock();
    }
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
    requests = new ConcurrentLinkedQueue<>();
    responses = new ConcurrentLinkedQueue<>();
    unprocessedRequests = new ConcurrentHashMap<>();
    bundleElementsByRequestId = new ConcurrentHashMap<>();

    stopwatch = Stopwatch.createStarted();
    callProcessor = new GRPCBidiCallProcessor<>();
    callProcessor.start(requests, resolveResponseReceiver, starter);
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

    unprocessedRequests.put(requestId, request);
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
    bundleSizeMetric.update(bundleSize);

    do {
      callProcessor.allRequestsAreQueued();
      boolean successfullyProcessed = callProcessor.waitUntilCompletion(Duration.standardMinutes(7));

      boolean allRequestsAreProcessed = unprocessedRequests.size() == 0;
      if (allRequestsAreProcessed) {
        break;
      }

      if (successfullyProcessed) {
        logger.warn(
            "Something is not right - call processor returned success but there unprocessed requests");
      }

      gRPCFailures.inc();

      requests = new ConcurrentLinkedQueue<>();
      unprocessedRequests.forEach((requestId, request) -> requests.add(request));
      logger.warn(
          "Failed to process the gRPC call. Will retry remaining " + requests.size()
              + " requests.");

      callProcessor = new GRPCBidiCallProcessor<>();
      callProcessor.start(requests, resolveResponseReceiver, starter);
    } while (true);

    logger.debug("gRPC processing is completed.");
    stopwatch.stop();
    grpcDurationMetric.update(stopwatch.elapsed(TimeUnit.MILLISECONDS) / bundleSize);

    outputReceivedResults(context);
  }

  private void outputReceivedResults(DoFn<String, PartialAddress>.FinishBundleContext context) {
    responses.forEach(response -> {
          var requestId = response.getRequestId();
          var pCollectionElement = bundleElementsByRequestId.remove(requestId);
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
    channelLock.lock();
    try {
      if (--numberOfChannelUsers > 0) {
        logger.info(
            "There are additional gRPC channel users, leaving it open for " + this.getClass()
                .getName());
        return;
      }
      if (channel == null) {
        return;
      }
      logger.info("Shutting down the gRPC channel for " + this.getClass().getName());
      channel.shutdown();
      try {
        channel.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("Interrupted while terminating gRPC channel:", e);
      }
      channel = null;
    } finally {
      channelLock.unlock();
    }
  }
}

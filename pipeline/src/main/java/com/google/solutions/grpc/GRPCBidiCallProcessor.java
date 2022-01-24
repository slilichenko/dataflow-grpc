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

package com.google.solutions.grpc;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GRPCBidiCallProcessor<Request extends GeneratedMessageV3, Response extends GeneratedMessageV3> {

  private final AtomicBoolean allRequestsQueued = new AtomicBoolean(false);
  private ResponseReceiver<Response> responseReceiver;
  private CountDownLatch allResponsesProcessed;
  private Throwable failureReason;

  public interface BidiMethodStarter<Request, Response> {

    void start(ClientResponseObserver<Request, Response> clientResponseObserver);
  }

  public interface ResponseReceiver<Response> {

    void receive(Response response);
  }

  private static final Logger logger = LoggerFactory.getLogger(GRPCBidiCallProcessor.class);

  public void start(Queue<Request> requests,
      ResponseReceiver<Response> responseReceiver,
      BidiMethodStarter<Request, Response> methodStarter) {
    this.responseReceiver = responseReceiver;

    allResponsesProcessed = new CountDownLatch(1);
    // When using manual flow-control and back-pressure on the client, the ClientResponseObserver handles both
    // request and response streams.
    ClientResponseObserver<Request, Response> clientResponseObserver =
        new BidiResponseObserver(requests);

    methodStarter.start(clientResponseObserver);
  }

  public void allRequestsAreQueued() {
    allRequestsQueued.set(true);
  }

  public boolean waitUntilCompletion(Duration duration) {
    while (true) {
      try {
        boolean success = allResponsesProcessed.await(duration.getMillis(), TimeUnit.MILLISECONDS);
        if (success) {
          // TODO: need to do more with failure reasons - differentiate between transient and permanent
          return failureReason == null;
        } else {
          logger.warn("Timeout exceeded");
          return false;
          // throw new RuntimeException("Timeout exceeded");
        }
      } catch (InterruptedException e) {
        logger.warn("Unexpected exception: ", e);
      }
    }
  }

  private class BidiResponseObserver implements ClientResponseObserver<Request, Response> {

    private final Queue<Request> requests;
    ClientCallStreamObserver<Request> requestStream;

    public BidiResponseObserver(Queue<Request> requests) {
      this.requests = requests;
    }

    @Override
    public void beforeStart(final ClientCallStreamObserver<Request> requestStream) {
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
          Request request = requests.poll();
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
    public void onNext(Response response) {
      if (logger.isDebugEnabled()) {
        logger.debug("<-- " + response);
      }
      responseReceiver.receive(response);
      // Signal the sender to send one message. TODO: test increasing this response
      requestStream.request(1);
    }

    @Override
    public void onError(Throwable e) {
      // TODO: figure out if the pipeline should fail if this happens.
      logger.error("Failed to process requests", e);
      failureReason = e;
      allResponsesProcessed.countDown();
    }

    @Override
    public void onCompleted() {
      logger.debug("gRPC processing is complete.");
      allResponsesProcessed.countDown();
    }
  }

}

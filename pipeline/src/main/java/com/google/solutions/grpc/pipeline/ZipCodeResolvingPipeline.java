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

import com.google.solutions.grpc.pipeline.model.PartialAddress;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

public class ZipCodeResolvingPipeline {

  interface Options extends PipelineOptions {

    @Required
    String getGrpcHost();

    void setGrpcHost(String value);

    @Required
    String getSubscription();

    void setSubscription(String value);

    @Required
    String getOutputBucket();

    void setOutputBucket(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.as(Options.class);

    Pipeline pipeline = Pipeline.create(options);
    PCollectionTuple resolutionOutcome = pipeline
        .apply("Read from PubSub", PubsubIO.readStrings().fromSubscription(
            options.getSubscription()))
        .apply("Resolve Zip", ParDo.of(
                new ZipResolverDoFn(options.getGrpcHost(), 443, false))
            .withOutputTags(ZipResolverDoFn.successfullyResolvedTag,
                TupleTagList.of(ZipResolverDoFn.failedToResolveTag)));

    PCollection<PartialAddress> successfullyResolved = resolutionOutcome.get(
        ZipResolverDoFn.successfullyResolvedTag);
    successfullyResolved
        .apply("Convert Address to CSV", ParDo.of(new DoFn<PartialAddress, String>() {
          @ProcessElement
          public void process(@Element PartialAddress address, OutputReceiver<String> out) {
            out.output(address.getZip() + '|' + address.getState() + '|' + address.getCity());
          }
        }))
        .apply("Fixed Window", Window.into(FixedWindows.of(Duration.standardMinutes(5))))
        .apply("Save Successfully Resolved",
            TextIO.write().to(options.getOutputBucket() + "/resolved").withSuffix("csv")
                .withNumShards(5));

    PCollection<KV<String, String>> failedToResolve = resolutionOutcome.get(
        ZipResolverDoFn.failedToResolveTag);
    failedToResolve
        .apply("Convert Failed to CSV", ParDo.of(new DoFn<KV<String, String>, String>() {
          @ProcessElement
          public void process(@Element KV<String, String> failedLookup,
              OutputReceiver<String> out) {
            out.output(failedLookup.getKey() + '|' + failedLookup.getValue());
          }
        }))
        .apply("Fixed Window", Window.into(FixedWindows.of(Duration.standardMinutes(5))))
        .apply("Save Failed to Resolved",
            TextIO.write().to(options.getOutputBucket() + "/failed").withSuffix("csv")
                .withNumShards(5));
  }

}

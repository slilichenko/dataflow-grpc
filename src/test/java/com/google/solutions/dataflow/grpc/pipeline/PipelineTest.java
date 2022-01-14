/*
 * Copyright 2016 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.dataflow.grpc.pipeline;

import com.google.solutions.dataflow.grpc.model.PartialAddress;
import com.google.solutions.dataflow.grpc.server.ZipResolverServer;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.print.attribute.standard.MediaSize.Other;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class PipelineTest {

  private static final int PORT = 50001;

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  private ZipResolverServer zipResolverServer;

  @Before
  public void setUp() throws IOException {

    zipResolverServer = new ZipResolverServer();
    zipResolverServer.runWithoutWaitingForTermination(new String[]{String.valueOf(PORT)});
    new Thread(() -> {
      try {
        zipResolverServer.awaitTermination();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).start();
  }

  @After
  public void tearDown() {
    zipResolverServer.shutdown();
  }

  @Test
  public void testZipCodeResolverDoFn() {
    final var knownAddresses = Arrays.asList(
        PartialAddress.create("94005", "CA", "San Francisco"),
        PartialAddress.create("94040", "CA", "Mountain View"),
        PartialAddress.create("96732", "HI", "Kahului"),
        PartialAddress.create("10001", "NY", "New York"));

    knownAddresses.forEach(
        address -> zipResolverServer.addDataEntry(
            address.getZip(), address.getState(), address.getCity()));

    final var validZips = knownAddresses.stream().map(address -> address.getZip()).collect(
        Collectors.toList());

    final var invalidZips = Arrays.asList("abc", "99999");

    final var zipsToProcess = new ArrayList<String>();
    zipsToProcess.addAll(validZips);
    zipsToProcess.addAll(invalidZips);

    PCollectionTuple resolutionOutcome = testPipeline
        .apply("Create requests", Create.of(zipsToProcess))
        .apply("Resolve Zip", ParDo.of(new ZipResolverDoFn())
            .withOutputTags(ZipResolverDoFn.successfullyResolvedTag,
                TupleTagList.of(ZipResolverDoFn.failedToResolveTag)));

    PCollection<PartialAddress> resolvedAddresses = resolutionOutcome.get(
        ZipResolverDoFn.successfullyResolvedTag);
    PCollection<KV<String, String>> failedToResolve = resolutionOutcome.get(
        ZipResolverDoFn.failedToResolveTag);

    PAssert.that("Resolve addresses", resolvedAddresses).containsInAnyOrder(
        knownAddresses
    );

    List<KV<String, String>> expectedFailures = invalidZips.stream()
        .map(zip -> KV.of(zip, "Not found")).collect(Collectors.toList());
    PAssert.that("Failed zips", failedToResolve).containsInAnyOrder(expectedFailures);

    testPipeline.run();
  }


}

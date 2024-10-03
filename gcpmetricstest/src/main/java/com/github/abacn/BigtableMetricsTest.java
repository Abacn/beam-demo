package com.github.abacn;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;

// ./gradlew :gcpmetricstest:run --args='--project=google.com:clouddfe --region=us-central1 --tempLocation=gs://clouddfe-yihu-test/tmp --runner=DirectRunner'

// ./gradlew :gcpmetricstest:run --args='--project=google.com:clouddfe --region=us-central1 --tempLocation=gs://clouddfe-yihu-test/tmp --runner=DataflowRunner --dataflowServiceOptions=worker_utilization_hint=0.9'

/**
 * Test throttling metrics on Bigtable
 */
public class BigtableMetricsTest {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableMetricsTest.class);
  private static String project = "google.com:clouddfe";

  private static String instanceId = "bt-write-xlang-20240307-2f2eeeb3";
  // bt-write-xlang-20240307-2b8bfce8
  // bt-write-xlang-20240307-2f2eeeb3

  // cbt -project google.com:clouddfe -instance bt-write-xlang-20240307-2b8bfce8 createtable testtable2 families=cf:maxage=10d||maxversions=1
  private static String tableId = "testtable1";

  private static int numRows = 200_000_000;

  private static int valueSizeBytes = 1_000;
  private static String READ_ELEMENT_METRIC_NAME = "BEAM_METRICS";

  private static final String COLUMN_FAMILY_NAME = "cf";

  private static void testRead(String[] argv) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(argv).create();
    Pipeline p = Pipeline.create(options);

    BigtableIO.Read readIO =
        BigtableIO.read()
            .withoutValidation()
            .withProjectId(project)
            .withInstanceId(instanceId)
            .withTableId(tableId);

    p.apply("Read from BigTable", readIO)
        .apply("Counting element", ParDo.of(new CountingFn<>("read-element")));

    p.run().waitUntilFinish();
  }

  private static void testWrite(String[] argv) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(argv).create();
    Pipeline p = Pipeline.create(options);

    BigtableIO.Write writeIO =
        BigtableIO.write()
            .withProjectId(project)
            .withInstanceId(instanceId)
            .withTableId(tableId);

    p.apply(GenerateSequence.from(0).to(numRows))
        .apply("Map records", ParDo.of(new MapToBigTableFormat(valueSizeBytes)))
        .apply("Write to BigTable", writeIO);

    p.run().waitUntilFinish();
  }

  /** A utility DoFn that counts elements passed through. */
  public static final class CountingFn<T> extends DoFn<T, T> {

    private final Counter elementCounter;

    public CountingFn(String name) {
      elementCounter = Metrics.counter(READ_ELEMENT_METRIC_NAME, name);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      elementCounter.inc(1L);
      ctx.output(ctx.element());
    }
  }


  /** Maps long number to the BigTable format record. */
  private static class MapToBigTableFormat extends DoFn<Long, KV<ByteString, Iterable<Mutation>>>
      implements Serializable {

    private final int valueSizeBytes;

    public MapToBigTableFormat(int valueSizeBytes) {
      this.valueSizeBytes = valueSizeBytes;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Long index = c.element();

      ByteString key = ByteString.copyFromUtf8(String.format("key%09d", index));
      Random random = new Random(index);
      byte[] valBytes = new byte[this.valueSizeBytes];
      random.nextBytes(valBytes);
      ByteString value = ByteString.copyFrom(valBytes);

      Iterable<Mutation> mutations =
          ImmutableList.of(
              Mutation.newBuilder()
                  .setSetCell(
                      Mutation.SetCell.newBuilder()
                          .setValue(value)
                          .setTimestampMicros(Instant.now().toEpochMilli() * 1000L)
                          .setFamilyName(COLUMN_FAMILY_NAME))
                  .build());
      c.output(KV.of(key, mutations));
    }
  }

  private static class RateLimitingFn<T> extends DoFn<T, T> implements Serializable {

    private final long targetNumElement;

    private final long targetDurationMillis;

    RateLimitingFn(long targetNumElement, long targetDurationMillis) {
      this.targetNumElement = targetNumElement;
      this.targetDurationMillis = targetDurationMillis;
    }

    @StateId("ratelimiting.count") private final StateSpec<ValueState<Long>> numReadSinceLast = StateSpecs.value();

    @StateId("ratelimiting.time") private final StateSpec<ValueState<Long>> lastTimeMillis = StateSpecs.value();

    private final Counter throtCounter = Metrics.counter("dataflow-throttling-metrics", "throttling-msecs");

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("ratelimiting.count") ValueState<Long> numReadState,
        @StateId("ratelimiting.time") ValueState<Long> lastTimeState) throws InterruptedException {
      long lastValue = MoreObjects.firstNonNull(numReadState.read(), 0L);
      lastTimeState.write(lastValue + 1);
      LOG.warn("lastValue: {}", lastValue);
      if (lastValue >= targetNumElement) {
        long lastTime = MoreObjects.firstNonNull(lastTimeState.read(), 0L);
        long nowTime = Instant.now().toEpochMilli();
        LOG.warn("lastTime: {}", lastTime);
        if (nowTime - lastTime > targetDurationMillis) {
          // reset
          numReadState.write(1L);
          lastTimeState.write(nowTime);
        } else {
          long timeSleep = targetDurationMillis - (nowTime - lastTime);
          throtCounter.inc(timeSleep);
          Thread.sleep(timeSleep);
        }
      }

      c.output(c.element());
    }
  }

  public static void main(String[] argv) {
    testWrite(argv);
    // testRead(argv);
  }
}

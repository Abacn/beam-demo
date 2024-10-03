package com.github.abacn;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.http.client.HttpResponseException;


// ./gradlew :gcpmetricstest:run --args='--project=... --region=us-central1 --tempLocation=gs://.../tmp --runner=DataflowRunner'

public class ThrottlingMetricsDemo {
  static boolean enableThrottlingCounter = true;

  static FakeClient client = new FakeClient();
  public static void main(String[] argv) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(argv).create();
    Pipeline p = Pipeline.create(options);
    final int numRows = 100_000_000;

    // Trigger creations of mock data
    p.apply(GenerateSequence.from(0).to(numRows))
        .apply(
            "Generates bytes",
            ParDo.of(
                new DoFn<Long, KV<Long, byte[]>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Long key = c.element();
                    Random rd = new Random(key);
                    byte[] value = new byte[1_000];
                    rd.nextBytes(value);
                    c.output(KV.of(key, value));
                  }
                }))
        .apply(
            "Process bytes",
            ParDo.of(
                new DoFn<KV<Long, byte[]>, Void>() {
                  private final Counter ThrottlingMsecs = Metrics.counter(
                      Metrics.THROTTLE_TIME_NAMESPACE,
                      Metrics.THROTTLE_TIME_COUNTER_NAME);

                  private final transient Random rd = new Random();

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Instant start = Instant.now();
                    try {
                      // Assume we use a client to make some API call to external resources
                      client.call(c.element().getKey(), c.element().getValue());
                    } catch (HttpResponseException e) {
                      long throttlingTime = Duration.between(start, Instant.now()).toMillis();
                      if (enableThrottlingCounter) {
                        // Report throttling time to Dataflow runner
                        ThrottlingMsecs.inc(throttlingTime);
                      }
                    }
                  }
                }));
    p.run();
  }

  /**
   * A client that will start to throttle after a minute of first call, mimics real world throttling issue when
   * service becomes throttled after Dataflow pipeline ramp up.
   */
  public static class FakeClient {
    private static Instant firstCall = null;

    public void call(long v, byte[] unused) throws HttpResponseException {
      if (firstCall == null) {
        firstCall = Instant.now();  // approximate, no need to be atomic
      }

      Random rd = new Random(v);
      Instant start = Instant.now();
      // some time-consuming operation
      while (Instant.now().isBefore(start.plusMillis(1))) {
        rd.nextInt();
      }
      // 10% call throttled after 1 minute
      if (rd.nextDouble() < 0.1 && Instant.now().isAfter(firstCall.plusMillis(60_000))) {
        long throttTime = (long) (rd.nextDouble() * 10);
        while (Instant.now().isBefore(start.plusMillis(throttTime))) {
          rd.nextInt();
        }
        throw new HttpResponseException(429, "resource exhausted");
      }
    }
  }
}

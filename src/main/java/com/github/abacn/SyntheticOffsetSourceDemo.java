package com.github.abacn;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.common.collect.ImmutableList;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class SyntheticOffsetSourceDemo {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SyntheticOffsetSourceDemo.class);
  private static final String NAMESPACE = SyntheticOffsetSourceDemo.class.getName();

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation().create();

    Pipeline p = Pipeline.create(options);

    p.apply(org.apache.beam.sdk.io.Read.from(new BoundedBundleSource(50, 2000, 1000)))
      .apply("Counting element", ParDo.of(new CountingFn<>(NAMESPACE, "read_element")));

    PipelineResult presult = p.run();
    presult.waitUntilFinish();

    Long metricResult = presult.metrics().queryMetrics(MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(NAMESPACE, "read_element"))
                    .build())
            .getCounters()
            .iterator().next().getAttempted();

    LOG.warn("Number of elements: {}", metricResult);
  }

  private static class CountingFn<T> extends DoFn<T, Void> {

    private final Counter elementCounter;

    CountingFn(String namespace, String name) {
      elementCounter = Metrics.counter(namespace, name);
    }

    @ProcessElement
    public void processElement() {
      elementCounter.inc(1L);
    }
  }
}

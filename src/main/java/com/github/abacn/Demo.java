package com.github.abacn;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.LoggerFactory;


public class Demo {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Demo.class);
  private static final String NAMESPACE = Demo.class.getName();

  // run on direct runner: ./gradlew run
  // run on dataflow runner: ./gradlew run --args="--runner=DataflowRunner --project=bigquery-readapi-beam-test --region=us-central1 --tempLocation=***"
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation().create();

    Pipeline p = Pipeline.create(options);
    PCollection<TableRow> input = p.apply(BigQueryIO.readTableRows()
            .from("bigquery-readapi-beam-test:tpcds_1G.item")
            .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
            .withSelectedFields(Lists.newArrayList("i_item_id", "i_item_desc"))
            .withFormat(DataFormat.AVRO));
    input.apply("Counting element", ParDo.of(new CountingFn<>(NAMESPACE, "read_element")));
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

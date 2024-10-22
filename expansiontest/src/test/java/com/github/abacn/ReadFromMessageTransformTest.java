package com.github.abacn;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.time.Instant;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class ReadFromMessageTransformTest {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFromMessageTransformTest.class);
  public @Rule TestPipeline p = TestPipeline.create();

  @Test
  public void testTransform() {
    Instant now = Instant.now();
    Message m = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
    p.apply(Create.of(m)).apply(new ReadFromMessageTransform()).apply(ParDo.of(new PrintFn()));

    p.run().waitUntilFinish();
  }

  static class PrintFn extends DoFn<String, Void> {
    @ProcessElement
    public void process(@Element String element) {
      LOG.info("received element {}", element);
    }
  }
}

package com.github.abacn;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import com.google.protobuf.Message;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

import java.util.Map;

// ./gradlew :expansiontest:shadowJar
// java -jar expansiontest/build/libs/expansiontest-all.jar
public class ReadFromMessageTransform extends PTransform<PCollection<Message>, PCollection<String>> {
  @Override
  public PCollection<String> expand(PCollection<Message> input) {
    return input.apply(ParDo.of(new MessageToStringFn()));
  }

  static class MessageToStringFn extends DoFn<Message, String> {
    @ProcessElement
    public void process(ProcessContext ctx) {
      Message m = ctx.element();
      String s = m.toString();
      ctx.output(s);
    }
  }

  public static class Configuration {

  }

  static class Builder
      implements ExternalTransformBuilder<Configuration, PCollection<Message>, PCollection<String>> {

    @Override
    public PTransform<PCollection<Message>, PCollection<String>> buildExternal(
        Configuration config) {
      return new ReadFromMessageTransform();
    }
  }

  @AutoService(ExternalTransformRegistrar.class)
  public static class External implements ExternalTransformRegistrar {
    public static final String URN =
        "beam:transform:com.github.abacn:proto_to_str:v1";

    @Override
    public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
      return ImmutableMap.of(URN, Builder.class);
    }
  }
}

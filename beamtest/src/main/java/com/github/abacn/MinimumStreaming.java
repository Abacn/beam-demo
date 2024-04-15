package com.github.abacn;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

// ./gradlew :beamtest:run --args='--project=google.com:clouddfe --region=us-central1 --tempLocation=gs://clouddfe-yihu-test/tmp --runner=DataflowRunner --streaming'
//
// ./gradlew :beamtest:shadowJar
// ./gradlew :beamtest:run --args='--project=google.com:clouddfe --region=us-central1 --tempLocation=gs://clouddfe-yihu-test/tmp --runner=DataflowRunner --filesToStage=/Users/yathu/dev/piece/beam-demo/beamtest/build/libs/beamtest-1.0-all.jar'
public class MinimumStreaming {

  public static void main(String[] argv) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(argv).create();
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(1, 2, 3, 4, 5));
    p.run().waitUntilFinish();
  }
}

package com.github.abacn;

import org.apache.beam.runners.core.metrics.StringSetData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// ./gradlew :beamtest:run --args='--project=google.com:clouddfe --region=us-central1 --tempLocation=gs://clouddfe-yihu-test/tmp --runner=DataflowRunner --streaming'
//
// ./gradlew :beamtest:shadowJar
// ./gradlew :beamtest:run --args='--project=google.com:clouddfe --region=us-central1 --tempLocation=gs://clouddfe-yihu-test/tmp --runner=DataflowRunner --experiments=use_runner_v2 --filesToStage=/Users/yathu/dev/piece/beam-demo/beamtest/build/libs/beamtest-1.0-all.jar'
public class MinimumStreaming {

  public static void main(String[] argv) {
    Set<String> original = new HashSet<>();
    original.add("1");

    new Thread(() -> {
      Random rand = new Random();
      while (true) {
        original.add(String.valueOf(rand.nextInt() % 10_000_000));
      }
    }).start();

    new Thread(() -> {
      while (true) {
        List<String> s = new ArrayList<>(original);
        int size = s.size();
        if (size % 10 == 0) {
          System.out.print(size);
          System.out.print(" ");
        }
      }
    }).start();

  }
}
